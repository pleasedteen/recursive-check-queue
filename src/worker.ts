import { randomUUID } from 'node:crypto';
import {
  ChildResult,
  ClaimedCheck,
  ExpansionAck,
  Lease,
  LeaseFencedError,
  serializeError,
  type CheckDescriptor,
  type SerializedError,
} from './types.js';
import { Queue } from './queue.js';

export interface WorkerBackend {
  claim(workerId: string): Promise<ClaimedCheck | null>;
  heartbeat(lease: Lease): Promise<unknown>;
  expand(
    lease: Lease,
    children: CheckDescriptor[],
    options?: { finalize?: boolean },
  ): Promise<ExpansionAck>;
  complete(
    lease: Lease,
    outcome: { status: 'completed' | 'failed'; result?: unknown; error?: unknown },
  ): Promise<unknown>;
  readAllChildResults(parentId: string): Promise<ChildResult[]>;
  replenish?(trigger: string): Promise<unknown>;
  close?(): Promise<void> | void;
}

export interface CheckContext {
  job: ClaimedCheck;
  input: unknown;
  /** Complete ordered direct-child results when this is a resume. */
  children: ChildResult[] | null;
  signal: AbortSignal;
  expand(children: CheckDescriptor[]): Promise<unknown>;
}

export type CheckHandler = (context: CheckContext) => Promise<unknown> | unknown;
export type HandlerMap = Record<string, CheckHandler>;

export interface WorkerOptions {
  workerId?: string;
  pollMs?: number;
  heartbeatMs?: number;
  replenishMs?: number | null;
  leaseMs?: number;
  /** Pause between handover segments (backpressure / observability). */
  handoverSegmentDelayMs?: number;
  onEvent?: (event: Record<string, unknown>) => void;
}

export function isFenced(error: unknown): boolean {
  return (
    error instanceof LeaseFencedError ||
    (typeof error === 'object' &&
      error !== null &&
      ((error as { code?: string }).code === 'LEASE_FENCED' ||
        (error as { statusCode?: number }).statusCode === 409))
  );
}

export class DirectQueueBackend implements WorkerBackend {
  constructor(readonly queue: Queue) {}

  async claim(workerId: string): Promise<ClaimedCheck | null> {
    return this.queue.claim(workerId);
  }

  async heartbeat(lease: Lease): Promise<unknown> {
    return this.queue.heartbeat(lease);
  }

  async expand(
    lease: Lease,
    children: CheckDescriptor[],
    options?: { finalize?: boolean },
  ): Promise<ExpansionAck> {
    return this.queue.expand(lease, { children, ...(options?.finalize ? { finalize: true } : {}) });
  }

  async complete(
    lease: Lease,
    outcome: {
      status: 'completed' | 'failed';
      result?: unknown;
      error?: unknown;
    },
  ): Promise<unknown> {
    return this.queue.complete(lease, {
      status: outcome.status,
      result: outcome.result,
      error: outcome.error,
    });
  }

  async readAllChildResults(parentId: string): Promise<ChildResult[]> {
    return this.queue.readAllChildResults(parentId);
  }

  async replenish(trigger: string): Promise<unknown> {
    return this.queue.replenish(trigger);
  }

  close(): void {
    this.queue.close();
  }
}

/**
 * One Worker instance is one OS process's single execution slot. Its run loop
 * awaits each handler before claiming again. Expanding a parent returns from
 * the handler and therefore releases the slot.
 */
export class Worker {
  readonly workerId: string;
  private readonly pollMs: number;
  private readonly heartbeatMs: number;
  private readonly replenishMs: number | null;
  private readonly handoverSegmentDelayMs: number;
  private readonly onEvent: (event: Record<string, unknown>) => void;
  private stopping = false;
  private active: ClaimedCheck | null = null;
  private replenishTimer: NodeJS.Timeout | null = null;

  constructor(
    private readonly backend: WorkerBackend,
    private readonly handlers: HandlerMap,
    options: WorkerOptions = {},
  ) {
    this.workerId = options.workerId ?? `worker-${process.pid}-${randomUUID().slice(0, 8)}`;
    this.pollMs = options.pollMs ?? 50;
    this.heartbeatMs = options.heartbeatMs ?? 2_000;
    this.replenishMs = options.replenishMs ?? null;
    this.handoverSegmentDelayMs = Math.max(0, options.handoverSegmentDelayMs ?? 0);
    this.onEvent = options.onEvent ?? (() => undefined);
  }

  get activeJobId(): string | null {
    return this.active?.id ?? null;
  }

  stop(): void {
    this.stopping = true;
    if (this.replenishTimer) clearInterval(this.replenishTimer);
  }

  async run(): Promise<void> {
    this.onEvent({ type: 'worker-start', workerId: this.workerId, pid: process.pid });
    if (this.replenishMs && this.backend.replenish) {
      this.replenishTimer = setInterval(() => {
        void this.backend.replenish?.('background-worker').catch((error) => {
          this.onEvent({ type: 'replenish-error', workerId: this.workerId, error: serializeError(error) });
        });
      }, this.replenishMs);
    }

    while (!this.stopping) {
      let job: ClaimedCheck | null = null;
      try {
        job = await this.backend.claim(this.workerId);
      } catch (error) {
        this.onEvent({ type: 'claim-error', workerId: this.workerId, error: serializeError(error) });
        await sleep(this.pollMs * 4);
        continue;
      }

      if (!job) {
        await sleep(this.pollMs);
        continue;
      }

      // There is deliberately no claim call while this handle is running.
      this.active = job;
      try {
        await this.handle(job);
      } catch (error) {
        this.onEvent({
          type: 'handle-error',
          workerId: this.workerId,
          checkId: job.id,
          error: serializeError(error),
        });
        await sleep(this.pollMs * 4);
      } finally {
        this.active = null;
      }
    }

    if (this.replenishTimer) clearInterval(this.replenishTimer);
    this.onEvent({ type: 'worker-stop', workerId: this.workerId, pid: process.pid });
  }

  private async handle(job: ClaimedCheck): Promise<void> {
    const controller = new AbortController();
    let leaseLost = false;
    let heartbeatInFlight = false;

    const heartbeatTimer = setInterval(() => {
      if (heartbeatInFlight) return;
      heartbeatInFlight = true;
      this.backend
        .heartbeat(job)
        .catch((error) => {
          if (isFenced(error)) {
            leaseLost = true;
            controller.abort();
            this.onEvent({
              type: 'lease-lost',
              workerId: this.workerId,
              checkId: job.id,
              error: serializeError(error),
            });
          }
        })
        .finally(() => {
          heartbeatInFlight = false;
        });
    }, this.heartbeatMs);

    let children: ChildResult[] | null = null;
    if (job.resumable) {
      try {
        children = await this.backend.readAllChildResults(job.id);
      } catch (error) {
        clearInterval(heartbeatTimer);
        if (!leaseLost && !isFenced(error)) {
          await this.safeFail(job, error);
        }
        return;
      }
    }

    let expanded = false;
    const context: CheckContext = {
      job,
      input: job.input,
      children,
      signal: controller.signal,
      expand: async (descriptors: CheckDescriptor[]) => {
        if (expanded) throw new Error('a check can only hand over one checklist');
        const ack = await this.handOverChecklist(job, descriptors, controller);
        expanded = true;
        return ack;
      },
    };

    this.onEvent({
      type: 'job-start',
      workerId: this.workerId,
      pid: process.pid,
      checkId: job.id,
      kind: job.kind,
      attempt: job.attempt,
      resumable: job.resumable,
      activeSlots: 1,
    });

    try {
      const handler = this.handlers[job.kind];
      if (!handler) throw new Error(`no fixture handler registered for kind: ${job.kind}`);
      const output = await handler(context);

      if (leaseLost) return;

      if (!expanded && isExpandOutput(output)) {
        await this.handOverChecklist(job, output.expand, controller);
        expanded = true;
      }

      if (expanded) {
        this.onEvent({
          type: 'job-deferred',
          workerId: this.workerId,
          checkId: job.id,
          activeSlots: 0,
        });
        return;
      }

      await this.backend.complete(job, { status: 'completed', result: output });
      this.onEvent({ type: 'job-completed', workerId: this.workerId, checkId: job.id });
    } catch (error) {
      if (leaseLost || isFenced(error)) {
        this.onEvent({
          type: 'stale-submission-rejected',
          workerId: this.workerId,
          checkId: job.id,
          error: serializeError(error),
        });
        return;
      }
      await this.safeFail(job, error);
    } finally {
      clearInterval(heartbeatTimer);
    }
  }

  /**
   * Hand a whole checklist over through the existing expand interface, in
   * bounded segments of at most the queue's readyLimit. Small lists still use
   * a single call. The final segment seals the handover.
   *
   * If the lease was reclaimed while an earlier handover was open (e.g. after
   * a hard stop mid-handover), the restarted worker resumes from the durable
   * cursor; the skipped prefix is already registered. Re-sending a prefix is
   * also accepted because the queue ignores duplicate entries idempotently.
   */
  private async handOverChecklist(
    job: ClaimedCheck,
    descriptors: CheckDescriptor[],
    controller: AbortController,
  ): Promise<ExpansionAck> {
    const limit = Math.max(1, job.readyLimit ?? 1);
    let ack: ExpansionAck | null = null;

    // Resume mode: an open handover survived a crash/lease loss. Skip the
    // prefix the previous execution already durably registered.
    const start = job.handoverOpen ? Math.min(job.handoverNextOrdinal, descriptors.length) : 0;
    const pending = descriptors.slice(start);

    if (job.handoverOpen) {
      this.onEvent({
        type: 'handover-resume',
        workerId: this.workerId,
        checkId: job.id,
        resumeFromOrdinal: start,
        totalDescriptors: descriptors.length,
      });
    }

    if (pending.length === 0) {
      // Nothing left to register (or an empty checklist): seal immediately.
      ack = await this.backend.expand(job, [], { finalize: true });
      this.onEvent({
        type: 'handover-sealed',
        workerId: this.workerId,
        checkId: job.id,
        segments: 0,
        totalChildren: ack.totalChildren,
        admitted: ack.admitted,
      });
      return ack;
    }

    let segmentIndex = 0;
    for (let offset = 0; offset < pending.length; offset += limit) {
      if (controller.signal.aborted) {
        throw new LeaseFencedError('lease lost while handing over checklist');
      }
      const segment = pending.slice(offset, offset + limit);
      const isLast = offset + limit >= pending.length;
      ack = await this.backend.expand(job, segment, { finalize: isLast });
      segmentIndex += 1;
      this.onEvent({
        type: 'handover-segment',
        workerId: this.workerId,
        checkId: job.id,
        segment: segmentIndex,
        segmentSize: segment.length,
        nextOrdinal: ack.nextOrdinal,
        sealed: ack.sealed,
        ignored: ack.ignored,
      });
      if (!isLast && this.handoverSegmentDelayMs > 0) {
        await sleep(this.handoverSegmentDelayMs);
      }
    }

    const finalAck = ack!;
    this.onEvent({
      type: 'handover-sealed',
      workerId: this.workerId,
      checkId: job.id,
      segments: segmentIndex,
      totalChildren: finalAck.totalChildren,
      admitted: finalAck.admitted,
    });
    return finalAck;
  }

  private async safeFail(job: ClaimedCheck, error: unknown): Promise<void> {
    try {
      await this.backend.complete(job, { status: 'failed', error });
      this.onEvent({
        type: 'job-failed',
        workerId: this.workerId,
        checkId: job.id,
        error: serializeError(error),
      });
    } catch (completeError) {
      if (!isFenced(completeError)) throw completeError;
      this.onEvent({
        type: 'stale-submission-rejected',
        workerId: this.workerId,
        checkId: job.id,
        error: serializeError(completeError),
      });
    }
  }
}

function isExpandOutput(value: unknown): value is { expand: CheckDescriptor[] } {
  return (
    typeof value === 'object' &&
    value !== null &&
    Array.isArray((value as { expand?: unknown }).expand)
  );
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

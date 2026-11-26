import { randomUUID } from 'node:crypto';
import {
  AppendAck,
  ChildResult,
  ClaimedCheck,
  ExpansionAck,
  ExpansionStateName,
  Lease,
  LeaseFencedError,
  serializeError,
  type CheckDescriptor,
  type SerializedError,
} from './types.js';
import { Queue } from './queue.js';

export interface BeginExpansionAck {
  id: string;
  state: ExpansionStateName;
  registered: number;
  readLimit: number;
}

export interface WorkerBackend {
  claim(workerId: string): Promise<ClaimedCheck | null>;
  heartbeat(lease: Lease): Promise<unknown>;
  /** Legacy one-shot handover: register the whole array, then confirm. */
  expand(lease: Lease, children: CheckDescriptor[]): Promise<unknown>;
  beginExpansion(lease: Lease): Promise<BeginExpansionAck>;
  appendExpansion(lease: Lease, children: CheckDescriptor[]): Promise<AppendAck>;
  commitExpansion(lease: Lease): Promise<ExpansionAck>;
  complete(
    lease: Lease,
    outcome: { status: 'completed' | 'failed'; result?: unknown; error?: unknown },
  ): Promise<unknown>;
  readAllChildResults(parentId: string): Promise<ChildResult[]>;
  replenish?(trigger: string): Promise<unknown>;
  close?(): Promise<void> | void;
}

/**
 * Segmented checklist writer handed to a streaming handler. `append` splits
 * oversized arrays so no single durable registration decodes more than the
 * queue's ready limit. `registered` is the durable offset, letting an
 * interrupted handover resume without replaying committed segments.
 */
export interface ChecklistWriter {
  readonly registered: number;
  readonly readLimit: number;
  append(descriptors: CheckDescriptor[]): Promise<AppendAck>;
  commit(): Promise<ExpansionAck>;
}

export interface CheckContext {
  job: ClaimedCheck;
  input: unknown;
  /** Complete ordered direct-child results when this is a resume. */
  children: ChildResult[] | null;
  signal: AbortSignal;
  expand(children: CheckDescriptor[]): Promise<unknown>;
  /** Begin streaming a large checklist in bounded segments. */
  openChecklist(): Promise<ChecklistWriter>;
}

export type CheckHandler = (context: CheckContext) => Promise<unknown> | unknown;
export type HandlerMap = Record<string, CheckHandler>;

export interface WorkerOptions {
  workerId?: string;
  pollMs?: number;
  heartbeatMs?: number;
  replenishMs?: number | null;
  leaseMs?: number;
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

  async expand(lease: Lease, children: CheckDescriptor[]): Promise<unknown> {
    return this.queue.expand(lease, { children });
  }

  async beginExpansion(lease: Lease): Promise<BeginExpansionAck> {
    return this.queue.beginExpansion(lease);
  }

  async appendExpansion(lease: Lease, children: CheckDescriptor[]): Promise<AppendAck> {
    return this.queue.appendExpansion(lease, children);
  }

  async commitExpansion(lease: Lease): Promise<ExpansionAck> {
    return this.queue.commitExpansion(lease);
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
    let committed = false;
    let openWriter: ChecklistWriter | null = null;
    const context: CheckContext = {
      job,
      input: job.input,
      children,
      signal: controller.signal,
      expand: async (descriptors: CheckDescriptor[]) => {
        if (committed) throw new Error('checklist already confirmed for this check');
        if (job.expansion.state !== 'none') {
          throw new Error('checklist handover already registered for this check; use openChecklist()');
        }
        const ack = await this.backend.expand(job, descriptors);
        expanded = true;
        committed = true;
        return ack;
      },
      openChecklist: async (): Promise<ChecklistWriter> => {
        if (expanded || committed) throw new Error('a check can only hand over one checklist');
        expanded = true;
        const begin = await this.backend.beginExpansion(job);
        let registered = begin.registered;
        let closed = false;
        const writer: ChecklistWriter = {
          get registered() {
            return registered;
          },
          readLimit: begin.readLimit,
          append: async (descriptors: CheckDescriptor[]) => {
            if (closed) throw new Error('checklist already confirmed');
            // Never hand a segment larger than the queue's read limit to a
            // single durable registration, even if the caller passes a big array.
            const limit = Math.max(1, begin.readLimit);
            let last: AppendAck | null = null;
            for (let offset = 0; offset < descriptors.length; offset += limit) {
              const slice = descriptors.slice(offset, offset + limit);
              last = await this.backend.appendExpansion(job, slice);
              registered = last.registered;
            }
            if (last) return last;
            return {
              id: job.id,
              state: 'open',
              registered,
              added: 0,
              decoded: 0,
              readLimit: limit,
            };
          },
          commit: async () => {
            if (closed) throw new Error('checklist already confirmed');
            const ack = await this.backend.commitExpansion(job);
            closed = true;
            committed = true;
            return ack;
          },
        };
        openWriter = writer;
        return writer;
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
        await this.backend.expand(job, output.expand);
        expanded = true;
        committed = true;
      }

      if (expanded && !committed) {
        // A streaming handler opened a checklist but returned before
        // confirming it, so children would stay unclaimable forever. Surface
        // it as a handler fault rather than silently deferring the slot.
        throw new Error('checklist handover was opened but never confirmed');
      }

      if (expanded && openWriter) {
        this.onEvent({
          type: 'job-deferred',
          workerId: this.workerId,
          checkId: job.id,
          activeSlots: 0,
          handover: 'segmented',
        });
        return;
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

import type { CheckDescriptor } from './types.js';
import type { CheckContext, CheckHandler, HandlerMap } from './worker.js';

export interface FixtureTreeInput {
  depth?: number;
  breadth?: number;
  delayMs?: number;
  includeFailure?: boolean;
  label?: string;
  /** Descriptors handed over per durable registration segment. */
  segment?: number;
  /** Delay between segments to make an interrupted handover reproducible. */
  streamDelayMs?: number;
}

function interruptibleDelay(ms: number, signal: AbortSignal): Promise<void> {
  if (ms <= 0) return Promise.resolve();
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(new Error('aborted before delay'));
      return;
    }
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    const onAbort = (): void => {
      clearTimeout(timer);
      reject(new Error('lease lost; fixture work aborted'));
    };
    signal.addEventListener('abort', onAbort, { once: true });
  });
}

async function delay(context: CheckContext, factor = 1): Promise<void> {
  const input = (context.input ?? {}) as { delayMs?: number; ms?: number };
  const ms = input.delayMs ?? input.ms ?? 0;
  await interruptibleDelay(ms * factor, context.signal);
}

function treeChildren(input: FixtureTreeInput): CheckDescriptor[] {
  const depth = input.depth ?? 2;
  const breadth = input.breadth ?? 2;
  const children: CheckDescriptor[] = [];

  for (let i = 0; i < breadth; i += 1) {
    children.push({
      key: `b${i}`,
      kind: 'fixture.tree',
      input: {
        depth: depth - 1,
        breadth,
        delayMs: input.delayMs ?? 0,
        includeFailure: false,
        label: `${input.label ?? 'tree'}/b${i}`,
      } satisfies FixtureTreeInput,
    });
  }

  if (input.includeFailure) {
    children.push({
      key: 'required-bad-child',
      kind: 'fixture.fail',
      input: { message: 'intentional incompatible plugin fixture' },
    });
  }

  return children;
}

export interface FixtureWideInput {
  breadth?: number;
  includeFailure?: boolean;
  delayMs?: number;
  label?: string;
  /** Descriptors handed over per durable registration segment. */
  segment?: number;
  /** Delay between segments to make an interrupted handover reproducible. */
  streamDelayMs?: number;
}

function wideChildren(input: FixtureWideInput): CheckDescriptor[] {
  const breadth = input.breadth ?? 8;
  const children: CheckDescriptor[] = [];
  for (let i = 0; i < breadth; i += 1) {
    children.push({
      key: `w${i}`,
      kind: 'fixture.ok',
      input: {
        delayMs: input.delayMs ?? 0,
        label: `${input.label ?? 'wide'}/w${i}`,
      },
    });
  }
  if (input.includeFailure) {
    children.push({
      key: 'required-bad-child',
      kind: 'fixture.fail',
      input: { message: 'intentional wide fixture failure' },
    });
  }
  return children;
}

async function streamChecklist(
  context: CheckContext,
  descriptors: CheckDescriptor[],
  input: { segment?: number; streamDelayMs?: number },
): Promise<number> {
  const writer = await context.openChecklist();
  const segment = Math.max(1, Math.min(writer.readLimit, input.segment ?? writer.readLimit));
  let offset = writer.registered;
  for (; offset < descriptors.length; offset += segment) {
    await writer.append(descriptors.slice(offset, offset + segment));
    if (input.streamDelayMs && input.streamDelayMs > 0) {
      await interruptibleDelay(input.streamDelayMs, context.signal);
    }
  }
  const ack = await writer.commit();
  return ack.totalChildren;
}

export function createFixtureHandlers(): HandlerMap {
  const ok: CheckHandler = async (context) => {
    await delay(context);
    return {
      ok: true,
      kind: context.job.kind,
      key: context.job.key,
      handledByPid: process.pid,
      attempt: context.job.attempt,
      echo: context.input,
    };
  };

  return {
    'fixture.ok': ok,

    'fixture.fail': async (context) => {
      await delay(context);
      const input = (context.input ?? {}) as { message?: string };
      throw new Error(input.message ?? 'intentional fixture failure');
    },

    'fixture.slow': async (context) => {
      const input = (context.input ?? {}) as { ms?: number; delayMs?: number };
      const ms = input.ms ?? input.delayMs ?? 250;
      await interruptibleDelay(ms, context.signal);
      return { slow: true, ms, pid: process.pid };
    },

    'fixture.tree': async (context) => {
      const input = (context.input ?? {}) as FixtureTreeInput;
      const depth = input.depth ?? 2;

      // Resume: the worker has paged in every direct child result, including
      // terminal failures.
      if (context.children) {
        await delay(context, 0.25);
        return {
          tree: true,
          resumed: true,
          label: input.label ?? null,
          depth,
          receivedChildCount: context.children.length,
          childKeys: context.children.map((child) => child.key),
          failedChildren: context.children
            .filter((child) => child.status === 'failed')
            .map((child) => ({
              key: child.key,
              error: child.error?.message ?? 'failed',
            })),
          children: context.children.map((child) => ({
            key: child.key,
            ordinal: child.ordinal,
            status: child.status,
            result: child.result ?? null,
            error: child.error ?? null,
          })),
          handledByPid: process.pid,
        };
      }

      await delay(context, 0.5);

      // Leaf.
      if (depth <= 0) {
        return {
          leaf: true,
          label: input.label ?? null,
          key: context.job.key,
          checkedAt: new Date().toISOString(),
          handledByPid: process.pid,
        };
      }

      const descriptors = treeChildren({ ...input, depth });

      // Segmented handover: append bounded segments and confirm only when the
      // whole checklist is durable. A parent interrupted mid-stream resumes
      // from its durable registered offset instead of replaying a segment.
      const registered = await streamChecklist(context, descriptors, input);
      return {
        streamed: true,
        label: input.label ?? null,
        depth,
        registered,
        handledByPid: process.pid,
      };
    },

    'fixture.wide': async (context) => {
      const input = (context.input ?? {}) as FixtureWideInput;
      const descriptors = wideChildren(input);

      if (context.children) {
        await delay(context, 0.25);
        const failed = context.children.filter((child) => child.status === 'failed');
        return {
          wide: true,
          resumed: true,
          label: input.label ?? null,
          receivedChildCount: context.children.length,
          childKeys: context.children.map((child) => child.key),
          ordinals: context.children.map((child) => child.ordinal),
          failedChildren: failed.map((child) => ({
            key: child.key,
            error: child.error?.message ?? 'failed',
          })),
          children: context.children.map((child) => ({
            key: child.key,
            ordinal: child.ordinal,
            status: child.status,
            result: child.result ?? null,
            error: child.error ?? null,
          })),
          handledByPid: process.pid,
        };
      }

      await delay(context, 0.5);
      const registered = await streamChecklist(context, descriptors, input);
      return {
        streamed: true,
        wide: true,
        label: input.label ?? null,
        registered,
        handledByPid: process.pid,
      };
    },
  };
}

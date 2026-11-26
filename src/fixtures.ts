import type { CheckDescriptor } from './types.js';
import type { CheckContext, CheckHandler, HandlerMap } from './worker.js';

export interface FixtureTreeInput {
  depth?: number;
  breadth?: number;
  delayMs?: number;
  includeFailure?: boolean;
  label?: string;
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

      // First activation: hand over the segmented/bounded checklist. The
      // queue persists every descriptor but only read-admits a ready segment.
      return { expand: treeChildren({ ...input, depth }) };
    },
  };
}

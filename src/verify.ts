import assert from 'node:assert/strict';
import { spawn, type ChildProcess } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { DatabaseSync } from 'node:sqlite';
import { HttpQueueClient } from './client.js';
import { createFixtureHandlers } from './fixtures.js';
import { Queue } from './queue.js';
import type { QueueStats } from './types.js';
import { DirectQueueBackend, Worker, sleep } from './worker.js';

const here = dirname(fileURLToPath(import.meta.url));
const cliPath = resolve(here, 'cli.js');
const nodeExecutable = process.execPath;
const startedAt = Date.now();
const tempRoot = mkdtempSync(join(tmpdir(), 'plugin-check-verify-'));
const activeHandles = new Set<SpawnedProcess>();

function log(message: string): void {
  process.stdout.write(`[verify ${((Date.now() - startedAt) / 1000).toFixed(1)}s] ${message}\n`);
}

interface SpawnedProcess {
  child: ChildProcess;
  events: any[];
  stderr: string[];
  exited: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
}

function spawnCli(args: string[], label: string): SpawnedProcess {
  const child = spawn(nodeExecutable, ['--no-warnings', cliPath, ...args], {
    stdio: ['ignore', 'pipe', 'pipe'],
    env: { ...process.env, FORCE_COLOR: '0' },
  });
  const events: any[] = [];
  const stderr: string[] = [];

  child.stdout?.setEncoding('utf8');
  child.stdout?.on('data', (chunk: string) => {
    for (const line of chunk.split('\n')) {
      if (!line.trim()) continue;
      try {
        const event = JSON.parse(line);
        events.push(event);
        if (process.env.VERIFY_VERBOSE) log(`${label} ${line}`);
      } catch {
        events.push({ type: 'raw', raw: line });
      }
    }
  });
  child.stderr?.setEncoding('utf8');
  child.stderr?.on('data', (chunk: string) => {
    for (const line of chunk.split('\n')) {
      if (line.trim()) stderr.push(line);
      if (process.env.VERIFY_VERBOSE) process.stderr.write(`[${label}] ${line}\n`);
    }
  });

  const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolveExit) => {
    child.once('exit', (code, signal) => {
      activeHandles.delete(handle);
      resolveExit({ code, signal });
    });
  });

  const handle: SpawnedProcess = { child, events, stderr, exited };
  activeHandles.add(handle);
  return handle;
}

async function waitFor<T>(
  fn: () => Promise<T | null | undefined | false> | T | null | undefined | false,
  options: { timeoutMs?: number; intervalMs?: number; label?: string } = {},
): Promise<T> {
  const timeoutMs = options.timeoutMs ?? 20_000;
  const intervalMs = options.intervalMs ?? 50;
  const start = Date.now();
  let lastError: unknown;

  while (Date.now() - start < timeoutMs) {
    try {
      const value = await fn();
      if (value) return value;
    } catch (error) {
      lastError = error;
    }
    await sleep(intervalMs);
  }
  throw new Error(
    `timed out waiting for ${options.label ?? 'condition'} after ${timeoutMs}ms: ${
      lastError instanceof Error ? lastError.message : String(lastError ?? '')
    }`,
  );
}

async function waitForHttpHealth(port: number): Promise<void> {
  await waitFor(
    async () => {
      const response = await fetch(`http://127.0.0.1:${port}/health`);
      return response.ok;
    },
    { timeoutMs: 10_000, label: 'http health' },
  );
}

async function startSpawnedServer(
  dbPath: string,
  readyLimit: number,
  leaseMs: number,
): Promise<{ process: SpawnedProcess; port: number }> {
  const serverProcess = spawnCli(
    [
      'serve',
      '--db',
      dbPath,
      '--port',
      '0',
      '--ready-limit',
      String(readyLimit),
      '--lease-ms',
      String(leaseMs),
      '--replenish-ms',
      '50',
    ],
    'server',
  );
  const listening = await waitFor(
    () => serverProcess.events.find((event) => event.type === 'listening') as { port: number } | undefined,
    { timeoutMs: 10_000, label: 'server listening' },
  );
  await waitForHttpHealth(listening.port);
  return { process: serverProcess, port: listening.port };
}

async function forceKill(processes: SpawnedProcess[]): Promise<void> {
  for (const processHandle of processes) {
    if (!processHandle.child.killed) processHandle.child.kill('SIGKILL');
  }
  await Promise.race([
    Promise.all(processes.map((p) => p.exited)),
    sleep(3_000).then(() => {
      throw new Error('SIGKILL process did not exit within 3s');
    }),
  ]);
}

function assertStatsWithinLimit(stats: QueueStats): void {
  assert.ok(stats.readyPeak <= stats.readyLimit, `ready peak ${stats.readyPeak} > ${stats.readyLimit}`);
  assert.ok(
    stats.readyCurrent <= stats.readyLimit,
    `ready current ${stats.readyCurrent} > ${stats.readyLimit}`,
  );
  for (const read of stats.listReads) {
    assert.ok(
      read.readCount <= stats.readyLimit,
      `${read.op} read ${read.readCount} > limit ${stats.readyLimit}`,
    );
  }
}

async function testLeaseFencing(dir: string): Promise<void> {
  log('1/7 lease expiry and stale fencing');
  const dbPath = join(dir, 'lease.sqlite');
  const queue = new Queue({ dbPath, readyLimit: 2, leaseMs: 60 });
  queue.submitRoot({ id: 'lease-root', kind: 'fixture.ok' });

  const oldJob = queue.claim('old-worker');
  assert.ok(oldJob);
  await sleep(90);

  assert.throws(() => queue.heartbeat(oldJob), /LEASE_FENCED|expired|generation/);
  assert.throws(
    () => queue.complete(oldJob, { status: 'completed', result: 'stale-result' }),
    /LEASE_FENCED|expired|generation/,
  );

  const reclaimed = queue.claim('new-worker');
  assert.ok(reclaimed);
  assert.equal(reclaimed.id, oldJob.id);
  assert.equal(reclaimed.attempt, oldJob.attempt + 1);
  assert.equal(reclaimed.fence, oldJob.fence + 1);
  assert.notEqual(reclaimed.leaseToken, oldJob.leaseToken);

  assert.throws(
    () => queue.complete(oldJob, { status: 'completed', result: 'stale-result' }),
    /LEASE_FENCED|generation/,
  );
  queue.complete(reclaimed, { status: 'completed', result: 'fresh-result' });
  const progress = await queue.waitForTerminal('lease-root', { timeoutMs: 2_000 });
  assert.equal(progress.status, 'completed');

  queue.submitRoot({ id: 'heartbeat-root', kind: 'fixture.ok' });
  const heartbeatJob = queue.claim('heartbeat-worker')!;
  await sleep(45);
  queue.heartbeat(heartbeatJob, { leaseMs: 300 });
  // Longer than the original 60ms lease, but inside the renewed generation.
  await sleep(100);
  queue.complete(heartbeatJob, { status: 'completed', result: 'renewed-ok' });
  const heartbeatProgress = await queue.waitForTerminal('heartbeat-root', { timeoutMs: 2_000 });
  assert.equal(heartbeatProgress.status, 'completed');
  queue.close();
}

function testFairnessSecondClaim(dir: string): void {
  log('2/7 cap=1 cross-tree fairness by second successful claim');
  const queue = new Queue({ dbPath: join(dir, 'fairness.sqlite'), readyLimit: 1 });

  queue.submitRoot({ id: 'tree-a', kind: 'fixture.tree' });
  const rootA = queue.claim('fair-worker');
  assert.ok(rootA);
  queue.expand(rootA, {
    children: Array.from({ length: 12 }, (_, index) => ({
      key: `a-${index}`,
      kind: 'fixture.ok',
    })),
  });

  const treeB = queue.submitRoot({ id: 'tree-b-single', kind: 'fixture.ok' });
  assert.equal(treeB.status, 'staged', 'B waits for the one-slot ready buffer');

  const firstClaimAfterSubmit = queue.claim('fair-worker');
  assert.ok(firstClaimAfterSubmit);
  assert.equal(firstClaimAfterSubmit.key, 'a-0');

  const secondClaimAfterSubmit = queue.claim('fair-worker');
  assert.ok(secondClaimAfterSubmit);
  assert.equal(secondClaimAfterSubmit.id, 'tree-b-single');

  const stats = queue.stats(10_000);
  assert.equal(stats.readyPeak, 1);
  assertStatsWithinLimit(stats);
  queue.close();
}

async function testLargeChecklistAndLimits(dir: string): Promise<void> {
  log('3/7 large checklist, paged complete results, and read limits');
  const dbPath = join(dir, 'limits.sqlite');
  const queue = new Queue({ dbPath, readyLimit: 4, leaseMs: 10_000 });
  queue.submitRoot({
    id: 'large-parent',
    kind: 'fixture.tree',
    input: { depth: 1, breadth: 120, delayMs: 1, label: 'large' },
  });

  const worker = new Worker(new DirectQueueBackend(queue), createFixtureHandlers(), {
    workerId: 'limit-worker',
    pollMs: 5,
    heartbeatMs: 60_000,
    replenishMs: 15,
  });
  const runPromise = worker.run();

  const progress = await queue.waitForTerminal('large-parent', { timeoutMs: 20_000, pollMs: 25 });
  assert.equal(progress.status, 'completed');
  const result = progress.result as { receivedChildCount: number; childKeys: string[] };
  assert.equal(result.receivedChildCount, 120);
  assert.equal(result.childKeys.length, 120);

  worker.stop();
  await runPromise;

  const stats = queue.stats(100_000);
  assert.equal(stats.readyPeak, 4, 'buffer should reach the configured cap');
  assertStatsWithinLimit(stats);
  assert.ok((stats.listReadMaxByOp.expand ?? 0) === 4);
  assert.ok((stats.listReadCountByOp.result_page ?? 0) >= 30, 'result pages should be bounded');
  log(
    `limit stats: readyPeak=${stats.readyPeak}, maxListReadsByOp=${JSON.stringify(
      stats.listReadMaxByOp,
    )}, resultPages=${stats.listReadCountByOp.result_page ?? 0}`,
  );
  queue.close();

  // Construct a saturated checklist and force the scheduling paths to have
  // multiple vacant slots, proving claim/replenish reads can reach but not
  // exceed the cap.
  const opDbPath = join(dir, 'operation-reads.sqlite');
  const opQueue = new Queue({ dbPath: opDbPath, readyLimit: 3 });
  opQueue.submitRoot({ id: 'op-root', kind: 'fixture.tree' });
  const opRoot = opQueue.claim('op-worker')!;
  const expansion = opQueue.expand(opRoot, {
    children: Array.from({ length: 24 }, (_, i) => ({ key: `c${i}`, kind: 'fixture.ok' })),
  });
  assert.equal(expansion.checklistRead, 3);

  const raw = new DatabaseSync(opDbPath);
  raw.exec('PRAGMA busy_timeout=10000');
  raw
    .prepare("UPDATE checks SET status='leased', lease_expires_at = ? WHERE status='ready'")
    .run(Date.now() + 60_000);

  const claim = opQueue.claim('op-worker');
  assert.ok(claim);
  const claimStats = opQueue.stats(100_000).listReads.filter((s) => s.op === 'claim');
  assert.equal(claimStats.at(-1)?.readCount, 3);

  raw
    .prepare("UPDATE checks SET status='leased', lease_expires_at = ? WHERE status='ready'")
    .run(Date.now() + 60_000);
  const replenish = opQueue.replenish('verify');
  assert.equal(replenish.checklistRead, 3);
  assertStatsWithinLimit(opQueue.stats(100_000));
  raw.close();
  opQueue.close();
}

function assertSingleSlotEvents(events: any[]): void {
  let active = 0;
  let maxActive = 0;
  for (const event of events) {
    if (event.type === 'job-start') {
      active += 1;
      maxActive = Math.max(maxActive, active);
      assert.equal(active, 1, 'worker process must never run two jobs concurrently');
    } else if (
      event.type === 'job-completed' ||
      event.type === 'job-failed' ||
      event.type === 'job-deferred'
    ) {
      active = Math.max(0, active - 1);
    }
  }
  assert.equal(maxActive, 1);
}

async function testSingleProcessCapOneTree(dir: string): Promise<void> {
  log('4/7 one real worker process, cap=1, multi-layer failure-bearing tree');
  const dbPath = join(dir, 'single-cap-one.sqlite');
  const queue = new Queue({ dbPath, readyLimit: 1, leaseMs: 10_000 });
  queue.submitRoot({
    id: 'deep-root',
    kind: 'fixture.tree',
    input: { depth: 4, breadth: 2, includeFailure: true, delayMs: 2, label: 'deep' },
  });

  const worker = spawnCli(
    [
      'worker',
      '--db',
      dbPath,
      '--fixtures',
      '--worker-id',
      'only-one-worker',
      '--ready-limit',
      '1',
      '--lease-ms',
      '10000',
      '--heartbeat-ms',
      '1000',
      '--poll-ms',
      '5',
      '--replenish-ms',
      '20',
    ],
    'single-worker',
  );

  await waitFor(
    () => worker.events.find((e) => e.type === 'worker-start' && e.workerId === 'only-one-worker'),
    { timeoutMs: 5_000, label: 'single worker start' },
  );

  const progress = await queue.waitForTerminal('deep-root', { timeoutMs: 30_000, pollMs: 40 });
  assert.equal(progress.status, 'completed', 'the resuming parent receives and contains child failure');
  const result = progress.result as {
    receivedChildCount: number;
    childKeys: string[];
    failedChildren: Array<{ key: string; error: string }>;
  };
  assert.equal(result.receivedChildCount, 3);
  assert.deepEqual(result.childKeys, ['b0', 'b1', 'required-bad-child']);
  assert.equal(result.failedChildren.length, 1);
  assert.equal(result.failedChildren[0]?.key, 'required-bad-child');
  assert.match(result.failedChildren[0]?.error ?? '', /intentional incompatible/);

  const stats = queue.stats(100_000);
  assert.equal(stats.readyPeak, 1);
  assert.deepEqual(stats.distinctClaimWorkers, ['only-one-worker']);
  assertStatsWithinLimit(stats);
  assertSingleSlotEvents(worker.events);

  worker.child.kill('SIGTERM');
  await worker.exited;
  queue.close();
}

interface MarkerRow {
  child_id: string;
  parent_id: string;
  status: string;
  completed_at: number;
}

/**
 * Drive a wide-checklist fixture (single worker, readyLimit=1) through two
 * interruptions: one while its checklist is still being registered, and one
 * after confirmation while the tail is still dispatching. Both must recover
 * through the parent's own lease, reuse finished child results, and deliver
 * children to the parent in original order, including the business failure.
 */
async function testSegmentedHandoverInterruptions(dir: string): Promise<void> {
  log('5/7 segmented handover: interrupt during registration and after confirmation');
  const dbPath = join(dir, 'handover.sqlite');
  const limit = 1;
  const leaseMs = 1_500;

  // Phase A — interrupt during registration with a real single worker process.
  // A short-lived worker is SIGKILLed mid-stream while its checklist is only
  // partially registered. Children stay unclaimable; a replacement worker
  // reclaims the parent through its own lease, resumes from the durable
  // registered offset, and the old lease cannot append to or confirm the new
  // generation's checklist.
  {
    const rootId = 'handover-wide-root';
    const breadth = 12;
    const shortLeaseMs = 400;
    const seed = new Queue({ dbPath, readyLimit: limit, leaseMs: shortLeaseMs });
    seed.submitRoot({
      id: rootId,
      kind: 'fixture.wide',
      input: {
        breadth,
        includeFailure: true,
        delayMs: 0,
        segment: 1,
        streamDelayMs: 40,
        label: rootId,
      },
    });
    seed.close();

    const oldWorker = spawnCli(
      [
        'worker',
        '--db',
        dbPath,
        '--fixtures',
        '--worker-id',
        'handover-old',
        '--ready-limit',
        String(limit),
        '--lease-ms',
        String(shortLeaseMs),
        '--heartbeat-ms',
        '100',
        '--poll-ms',
        '5',
      ],
      'handover-old',
    );

    const midStream = await waitFor(
      () => {
        const info = readLeaseRow(dbPath, rootId);
        if (
          info &&
          info.status === 'leased' &&
          info.worker_id === 'handover-old' &&
          info.expansion_state === 'open' &&
          info.expansion_registered >= 1 &&
          info.expansion_registered < breadth
        ) {
          return info;
        }
        return null;
      },
      { timeoutMs: 10_000, intervalMs: 5, label: 'mid-registration interruption point' },
    );
    log(`registration mid-stream: registered=${midStream.expansion_registered}/${breadth}`);

    // Before confirmation nothing is claimable.
    const counts = rawChildStatusCounts(dbPath, rootId);
    assert.equal(counts.ready, 0, 'no child is claimable before confirmation');
    assert.equal(counts.leased, 0, 'no child is leased before confirmation');
    assert.ok(counts.staged >= 1, 'registered prefix is staged');

    await forceKill([oldWorker]);

    // Wait past the dead worker's lease, then start the replacement.
    await sleep(shortLeaseMs + 100);
    const replacement = spawnCli(
      [
        'worker',
        '--db',
        dbPath,
        '--fixtures',
        '--worker-id',
        'handover-new',
        '--ready-limit',
        String(limit),
        '--lease-ms',
        String(shortLeaseMs),
        '--heartbeat-ms',
        '100',
        '--poll-ms',
        '5',
      ],
      'handover-new',
    );

    // The replacement reclaims the still-open parent and keeps the durable
    // prefix rather than replaying it.
    const takeover = await waitFor(
      () => {
        const info = readLeaseRow(dbPath, rootId);
        if (info && info.attempt >= 2 && info.worker_id === 'handover-new') return info;
        return null;
      },
      { timeoutMs: 10_000, intervalMs: 10, label: 'replacement takeover' },
    );
    assert.ok(
      takeover.expansion_registered >= midStream.expansion_registered,
      'durable registered prefix survives into the takeover',
    );

    // The fenced old generation cannot append to or confirm the new one.
    const oldLease = {
      id: rootId,
      workerId: midStream.worker_id!,
      leaseToken: midStream.lease_token!,
      fence: midStream.fence!,
    };
    const probe = new Queue({ dbPath, readyLimit: limit, leaseMs: shortLeaseMs });
    assert.throws(
      () => probe.appendExpansion(oldLease, [{ key: 'stale-extra', kind: 'fixture.ok', input: {} }]),
      /LEASE_FENCED|expired|generation/,
    );
    assert.throws(() => probe.commitExpansion(oldLease), /LEASE_FENCED|expired|generation/);
    probe.close();

    await waitFor(
      async () => {
        const info = new Queue({ dbPath, readyLimit: limit });
        try {
          const progress = info.getProgress(rootId);
          return progress.status === 'completed' || progress.status === 'failed' ? progress : null;
        } finally {
          info.close();
        }
      },
      { timeoutMs: 20_000, intervalMs: 25, label: 'handover phase A terminal' },
    ).then((progress) => {
      assert.equal(progress.status, 'completed');
      const result = progress.result as {
        receivedChildCount: number;
        childKeys: string[];
        ordinals: number[];
        failedChildren: Array<{ key: string }>;
      };
      const expectedKeys = Array.from({ length: breadth }, (_, i) => `w${i}`);
      expectedKeys.push('required-bad-child');
      assert.equal(result.receivedChildCount, breadth + 1);
      assert.deepEqual(result.childKeys, expectedKeys);
      assert.deepEqual(
        result.ordinals,
        expectedKeys.map((_, i) => i),
      );
      assert.equal(result.failedChildren.length, 1);
      assert.equal(result.failedChildren[0]?.key, 'required-bad-child');
    });

    replacement.child.kill('SIGTERM');
    await replacement.exited;

    const verifyQueue = new Queue({ dbPath, readyLimit: limit });
    const stats = verifyQueue.stats(100_000);
    assert.equal(stats.readyPeak, 1);
    assertStatsWithinLimit(stats);
    for (const read of stats.listReads.filter((r) => r.op === 'register')) {
      assert.ok(read.readCount <= limit, `register read ${read.readCount} > ${limit}`);
    }
    assert.ok((stats.listReadCountByOp.register ?? 0) >= breadth, 'registration volume is recorded');
    assert.ok(
      stats.distinctClaimWorkers.includes('handover-old') &&
        stats.distinctClaimWorkers.includes('handover-new'),
      'both worker generations claimed the parent',
    );
    verifyQueue.close();
  }

  // Phase B — interrupt after confirmation, mid tail dispatch, with the
  // service and its single worker force-killed together. A restarted service
  // and worker must keep dispatching the confirmed parent's staged tail,
  // reuse the child results acknowledged before the kill, and deliver every
  // child to the parent in original order, including the business failure.
  {
    const dbPathB = join(dir, 'handover-tail.sqlite');
    const rootId = 'handover-tail-root';
    const breadth = 10;
    const childDelayMs = 25;

    // Seed a confirmed checklist whose tail is still undispensed, then stop.
    const seed = new Queue({ dbPath: dbPathB, readyLimit: limit, leaseMs });
    seed.submitRoot({
      id: rootId,
      kind: 'fixture.wide',
      input: {
        breadth,
        includeFailure: true,
        delayMs: childDelayMs,
        segment: 1,
        label: rootId,
      },
    });
    const parent = seed.claim('tail-parent')!;
    assert.equal(seed.beginExpansion(parent).readLimit, limit);
    const descriptors: Array<{ key: string; kind: string; input: unknown }> = Array.from(
      { length: breadth },
      (_, i) => ({
        key: `w${i}`,
        kind: 'fixture.ok',
        input: { delayMs: childDelayMs },
      }),
    );
    descriptors.push({
      key: 'required-bad-child',
      kind: 'fixture.fail',
      input: { message: 'tail failure' },
    });
    for (const descriptor of descriptors) {
      seed.appendExpansion(parent, [descriptor]);
    }
    const commit = seed.commitExpansion(parent);
    assert.equal(commit.totalChildren, descriptors.length);
    const confirmed = seed.getProgress(rootId);
    assert.equal(confirmed.status, 'waiting');
    assert.ok(confirmed.staged >= 1, 'a confirmed tail is staged before any worker runs');
    seed.close();

    const leaseForTail = 800;
    const server = await startSpawnedServer(dbPathB, limit, leaseForTail);
    const client = new HttpQueueClient(`http://127.0.0.1:${server.port}`);
    const worker = spawnCli(
      [
        'worker',
        '--http',
        `http://127.0.0.1:${server.port}`,
        '--fixtures',
        '--worker-id',
        'tail-worker-1',
        '--poll-ms',
        '5',
        '--heartbeat-ms',
        '200',
        '--replenish-ms',
        '20',
      ],
      'tail-worker-1',
    );

    const beforeKill = await waitFor(
      async () => {
        const progress = await client.getProgress(rootId);
        if (progress.terminal >= 2 && progress.staged >= 1) return progress;
        return null;
      },
      { timeoutMs: 20_000, intervalMs: 20, label: 'tail dispatching before SIGKILL' },
    );
    assert.equal(beforeKill.status, 'waiting');
    log(
      `tail pre-kill: terminal=${beforeKill.terminal}, staged=${beforeKill.staged}, ` +
        `ready=${beforeKill.ready}, leased=${beforeKill.leased}`,
    );

    const marker = await readMarkerResult(dbPathB);
    assert.ok(marker, 'a tail child result is acknowledged before the kill');

    await forceKill([server.process, worker]);

    const restarted = await startSpawnedServer(dbPathB, limit, leaseForTail);
    const recoveredClient = new HttpQueueClient(`http://127.0.0.1:${restarted.port}`);
    const recoveryWorker = spawnCli(
      [
        'worker',
        '--http',
        `http://127.0.0.1:${restarted.port}`,
        '--fixtures',
        '--worker-id',
        'tail-worker-2',
        '--poll-ms',
        '10',
        '--heartbeat-ms',
        '200',
        '--replenish-ms',
        '30',
      ],
      'tail-worker-2',
    );

    const progress = await waitFor(
      async () => {
        const value = await recoveredClient.getProgress(rootId).catch(() => null);
        if (value && (value.status === 'completed' || value.status === 'failed')) return value;
        return null;
      },
      { timeoutMs: 30_000, intervalMs: 50, label: 'tail terminal after restart' },
    );

    await forceKill([recoveryWorker]);
    restarted.process.child.kill('SIGTERM');
    await restarted.process.exited.catch(() => undefined);

    assert.equal(progress.status, 'completed');
    const result = progress.result as {
      receivedChildCount: number;
      childKeys: string[];
      ordinals: number[];
      failedChildren: Array<{ key: string }>;
    };
    assert.equal(result.receivedChildCount, descriptors.length);
    assert.deepEqual(result.childKeys, descriptors.map((c) => c.key));
    assert.deepEqual(result.ordinals, descriptors.map((_, i) => i));
    assert.equal(result.failedChildren.length, 1);
    assert.equal(result.failedChildren[0]?.key, 'required-bad-child');

    const finalQueue = new Queue({ dbPath: dbPathB, readyLimit: limit });
    const stats = finalQueue.stats(100_000);
    assert.equal(stats.readyPeak, 1);
    assertStatsWithinLimit(stats);
    for (const read of stats.listReads.filter((r) => r.op === 'register')) {
      assert.ok(read.readCount <= limit, `register read ${read.readCount} > ${limit}`);
    }
    assert.ok(
      stats.distinctClaimWorkers.includes('tail-worker-2'),
      'the restarted worker continued dispatching the tail',
    );
    finalQueue.close();

    const rawFinal = new DatabaseSync(dbPathB);
    rawFinal.exec('PRAGMA busy_timeout=10000');
    const duplicateRows = rawFinal
      .prepare('SELECT child_id, COUNT(*) AS c FROM child_results GROUP BY child_id HAVING COUNT(*) > 1')
      .all();
    assert.equal(duplicateRows.length, 0, 'each tail child has exactly one result row');
    const markerAfter = rawFinal
      .prepare('SELECT completed_at FROM child_results WHERE child_id = ?')
      .get(marker!.child_id) as { completed_at: number } | undefined;
    assert.ok(markerAfter, 'pre-kill tail result survives');
    assert.equal(markerAfter.completed_at, marker!.completed_at, 'pre-kill tail result was reused');
    rawFinal.close();
  }
}

interface LeaseRow {
  status: string;
  worker_id: string | null;
  lease_token: string | null;
  fence: number | null;
  attempt: number;
  expansion_state: string | null;
  expansion_registered: number;
}

function readLeaseRow(dbPath: string, id: string): LeaseRow | null {
  const raw = new DatabaseSync(dbPath);
  raw.exec('PRAGMA busy_timeout=10000');
  const row = raw
    .prepare(
      `SELECT status, worker_id, lease_token, fence, attempt,
              expansion_state, expansion_registered
       FROM checks WHERE id = ?`,
    )
    .get(id) as Record<string, unknown> | undefined;
  raw.close();
  if (!row) return null;
  return {
    status: String(row.status),
    worker_id: (row.worker_id as string | null) ?? null,
    lease_token: (row.lease_token as string | null) ?? null,
    fence: row.fence === null ? null : Number(row.fence),
    attempt: Number(row.attempt),
    expansion_state: (row.expansion_state as string | null) ?? null,
    expansion_registered: Number(row.expansion_registered ?? 0),
  };
}

function readMarkerResult(dbPath: string): MarkerRow | undefined {
  const raw = new DatabaseSync(dbPath);
  raw.exec('PRAGMA busy_timeout=10000');
  const row = raw
    .prepare('SELECT child_id, parent_id, status, completed_at FROM child_results LIMIT 1')
    .get() as MarkerRow | undefined;
  raw.close();
  return row;
}

function rawChildStatusCounts(
  dbPath: string,
  parentId: string,
): { staged: number; ready: number; leased: number } {
  const raw = new DatabaseSync(dbPath);
  raw.exec('PRAGMA busy_timeout=10000');
  const rows = raw
    .prepare('SELECT status, COUNT(*) AS n FROM checks WHERE parent_id = ? GROUP BY status')
    .all(parentId) as Array<{ status: string; n: number | bigint }>;
  raw.close();
  const counts = { staged: 0, ready: 0, leased: 0 };
  for (const row of rows) {
    const n = typeof row.n === 'bigint' ? Number(row.n) : row.n;
    if (row.status in counts) counts[row.status as keyof typeof counts] = n;
  }
  return counts;
}


async function testMultiProcessKillAndRecovery(dir: string): Promise<void> {
  log('6/7 three worker processes: multiprocess claiming');
  const dbPath = join(dir, 'recovery.sqlite');
  const limit = 3;
  const server = await startSpawnedServer(dbPath, limit, 1_200);
  const client = new HttpQueueClient(`http://127.0.0.1:${server.port}`);

  const workers: SpawnedProcess[] = [];
  for (let i = 1; i <= 3; i += 1) {
    const worker = spawnCli(
      [
        'worker',
        '--http',
        `http://127.0.0.1:${server.port}`,
        '--fixtures',
        '--worker-id',
        `kill-w${i}`,
        '--poll-ms',
        '10',
        '--heartbeat-ms',
        '250',
        '--replenish-ms',
        '40',
      ],
      `kill-worker-${i}`,
    );
    workers.push(worker);
  }

  const rootIds = Array.from({ length: 8 }, (_, i) => `recovery-tree-${i}`);
  await Promise.all(
    rootIds.map((id, i) =>
      client.submitRoot({
        id,
        kind: 'fixture.tree',
        input: { depth: 3, breadth: 3, delayMs: 18 + (i % 3) * 10, label: id },
      }),
    ),
  );

  const beforeKill = await waitFor(
    async () => {
      const stats = await client.getStats();
      const oldWorkers = stats.distinctClaimWorkers.filter((id) => id.startsWith('kill-w'));
      if (
        oldWorkers.length >= 2 &&
        stats.claims >= 12 &&
        stats.statusCounts.completed >= 6 &&
        (stats.listReadCountByOp.expand ?? 0) >= 3
      ) {
        return stats;
      }
      return null;
    },
    { timeoutMs: 20_000, intervalMs: 100, label: 'multiprocess progress before SIGKILL' },
  );
  log(
    `pre-kill: claims=${beforeKill.claims}, completed=${beforeKill.statusCounts.completed}, ` +
      `workers=${beforeKill.distinctClaimWorkers.join(',')}`,
  );

  log('7/7 SIGKILL server and all workers; restart service and replacement workers');
  await forceKill([server.process, ...workers]);

  // Opening the database after SIGKILL exercises SQLite recovery and proves
  // acknowledged rows are present before any replacement process starts.
  const killedInspection = new Queue({ dbPath, readyLimit: limit });
  const killedStats = killedInspection.stats(100_000);
  assert.ok(killedStats.claims >= 12);
  assert.ok(killedStats.statusCounts.completed >= 6);
  assert.ok((killedStats.listReadCountByOp.expand ?? 0) >= 3);
  assertStatsWithinLimit(killedStats);

  const rawAfterKill = new DatabaseSync(dbPath);
  rawAfterKill.exec('PRAGMA busy_timeout=10000');
  const marker = rawAfterKill
    .prepare('SELECT child_id, parent_id, status, completed_at FROM child_results LIMIT 1')
    .get() as MarkerRow | undefined;
  assert.ok(marker, 'at least one acknowledged child result must survive SIGKILL');
  rawAfterKill.close();
  killedInspection.close();

  const restarted = await startSpawnedServer(dbPath, limit, 1_200);
  const recoveredClient = new HttpQueueClient(`http://127.0.0.1:${restarted.port}`);
  const recoveryWorkers: SpawnedProcess[] = [];
  for (let i = 1; i <= 3; i += 1) {
    recoveryWorkers.push(
      spawnCli(
        [
          'worker',
          '--http',
          `http://127.0.0.1:${restarted.port}`,
          '--fixtures',
          '--worker-id',
          `recovered-w${i}`,
          '--poll-ms',
          '15',
          '--heartbeat-ms',
          '300',
          '--replenish-ms',
          '60',
        ],
        `recovery-worker-${i}`,
      ),
    );
  }

  const progresses = await waitFor(
    async () => {
      const values = await Promise.all(
        rootIds.map((id) => recoveredClient.getProgress(id).catch(() => null)),
      );
      if (values.every((p): p is NonNullable<typeof p> => !!p && ['completed', 'failed'].includes(p.status))) {
        return values;
      }
      return null;
    },
    { timeoutMs: 45_000, intervalMs: 200, label: 'all recovery trees terminal' },
  );

  for (const progress of progresses) {
    assert.equal(progress.status, 'completed', `root ${progress.rootId}: ${JSON.stringify(progress)}`);
    assert.equal(progress.failed, 0);
    const result = progress.result as { receivedChildCount?: number };
    assert.equal(result.receivedChildCount, 3);
  }

  await forceKill(recoveryWorkers);
  restarted.process.child.kill('SIGTERM');
  await restarted.process.exited.catch(() => undefined);

  const finalQueue = new Queue({ dbPath, readyLimit: limit });
  const finalStats = finalQueue.stats(100_000);
  assertStatsWithinLimit(finalStats);
  assert.ok(finalStats.distinctClaimWorkers.filter((id) => id.startsWith('kill-w')).length >= 2);
  assert.ok(finalStats.distinctClaimWorkers.some((id) => id.startsWith('recovered-w')));

  const rawFinal = new DatabaseSync(dbPath);
  rawFinal.exec('PRAGMA busy_timeout=10000');
  const duplicateRows = rawFinal
    .prepare(
      'SELECT child_id, COUNT(*) AS c FROM child_results GROUP BY child_id HAVING COUNT(*) > 1',
    )
    .all();
  assert.equal(duplicateRows.length, 0, 'each logical child has exactly one result row');

  const markerAfter = rawFinal
    .prepare('SELECT status, completed_at FROM child_results WHERE child_id = ?')
    .get(marker!.child_id) as Pick<MarkerRow, 'status' | 'completed_at'> | undefined;
  assert.ok(markerAfter);
  assert.equal(markerAfter.completed_at, marker!.completed_at, 'pre-kill result was reused, not replaced');

  const checkCount = Number(
    (rawFinal.prepare('SELECT COUNT(*) AS n FROM checks').get() as { n: number | bigint }).n,
  );
  const resultCount = Number(
    (rawFinal.prepare('SELECT COUNT(*) AS n FROM child_results').get() as { n: number | bigint }).n,
  );
  assert.equal(checkCount, 8 * 40, 'depth=3 breadth=3 tree has 40 logical checks');
  assert.equal(resultCount, 8 * 39, 'every non-root logical child has one result');
  log(
    `recovery stats: claims=${finalStats.claims}, readyPeak=${finalStats.readyPeak}, ` +
      `workers=${finalStats.distinctClaimWorkers.length}, maxReads=${JSON.stringify(
        finalStats.listReadMaxByOp,
      )}`,
  );
  rawFinal.close();
  finalQueue.close();
}

async function main(): Promise<void> {
  log(`using temporary database directory ${tempRoot}`);
  try {
    await testLeaseFencing(tempRoot);
    await testFairnessSecondClaim(tempRoot);
    await testLargeChecklistAndLimits(tempRoot);
    await testSingleProcessCapOneTree(tempRoot);
    await testSegmentedHandoverInterruptions(tempRoot);
    await testMultiProcessKillAndRecovery(tempRoot);

    const elapsedMs = Date.now() - startedAt;
    log(`PASS: all persistence, recovery, fairness, and limit checks passed in ${(elapsedMs / 1000).toFixed(1)}s`);
    if (elapsedMs > 75_000) {
      throw new Error(`verification logic took ${elapsedMs}ms, leaving insufficient margin for the 80s command`);
    }
    rmSync(tempRoot, { recursive: true, force: true });
  } catch (error) {
    await forceKill(Array.from(activeHandles)).catch(() => undefined);
    console.error(`\nVERIFY FAILED. Temporary databases and logs remain in: ${tempRoot}`);
    console.error(error);
    process.exitCode = 1;
  }
}

await main();

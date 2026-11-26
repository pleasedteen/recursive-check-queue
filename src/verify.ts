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
import { startQueueServer } from './server.js';
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
  log('1/9 lease expiry and stale fencing');
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
  log('2/9 cap=1 cross-tree fairness by second successful claim');
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
  log('3/9 large checklist, paged complete results, and read limits');
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
    )}, resultPages=${stats.listReadCountByOp.result_page ?? 0} ` +
      '(实际读取量 actual checklist reads all <= readyLimit)',
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

async function testHandoffRetryDirect(dir: string): Promise<void> {
  log('4/9 handoff retry / 交接重试: same generation replays ack, registers once, stale generation fenced');

  // Small checklist against a larger cap: stats must report the rows actually
  // read (2), not the LIMIT/page capacity (4).
  const dbPath = join(dir, 'handoff-retry.sqlite');
  const queue = new Queue({ dbPath, readyLimit: 4, leaseMs: 60_000 });
  queue.submitRoot({ id: 'hr-root', kind: 'fixture.tree' });
  const job = queue.claim('hr-worker')!;
  assert.ok(job);

  const two = [
    { key: 'a', kind: 'fixture.ok' },
    { key: 'b', kind: 'fixture.ok' },
  ];
  const ack1 = queue.expand(job, { children: two });
  assert.equal(ack1.totalChildren, 2);
  assert.equal(ack1.checklistRead, 2, 'ack reports actual read rows');
  const expandRead = queue
    .stats(100_000)
    .listReads.filter((s) => s.op === 'expand' && s.parentId === 'hr-root')
    .at(-1);
  assert.equal(expandRead?.readCount, 2, 'stat reports actual read rows rather than the LIMIT');

  // Simulate a lost response packet: the exact same lease generation retries.
  const ack2 = queue.expand(job, { children: two });
  assert.deepEqual(ack2, ack1, 'retry replays the original handoff ack');
  const ack3 = queue.expand(job, { children: two });
  assert.deepEqual(ack3, ack1);

  const raw = new DatabaseSync(dbPath);
  raw.exec('PRAGMA busy_timeout=10000');
  const childCount = raw
    .prepare("SELECT COUNT(*) AS n FROM checks WHERE parent_id = 'hr-root'")
    .get() as { n: number | bigint };
  assert.equal(Number(childCount.n), 2, 'retry must not register duplicate children');
  const receiptCount = raw
    .prepare("SELECT COUNT(*) AS n FROM operation_receipts WHERE op = 'expand'")
    .get() as { n: number | bigint };
  assert.equal(Number(receiptCount.n), 1, 'exactly one durable handoff receipt');

  // A process presenting a stale/foreign lease generation cannot replay,
  // write, or dispatch through this handoff.
  assert.throws(
    () => queue.expand({ ...job, leaseToken: 'forged-token' }, { children: two }),
    /LEASE_FENCED|generation/,
  );
  assert.throws(
    () => queue.expand({ ...job, workerId: 'imposter' }, { children: two }),
    /LEASE_FENCED|generation/,
  );
  assert.throws(
    () => queue.expand({ ...job, fence: job.fence + 1 }, { children: two }),
    /LEASE_FENCED|generation/,
  );
  const childCountAfterRejects = raw
    .prepare("SELECT COUNT(*) AS n FROM checks WHERE parent_id = 'hr-root'")
    .get() as { n: number | bigint };
  assert.equal(Number(childCountAfterRejects.n), 2);
  raw.close();
  assertStatsWithinLimit(queue.stats(100_000));
  queue.close();

  // A never-committed expansion whose lease has since expired must be rejected;
  // the reclaimed generation can then hand off exactly once.
  const stalePath = join(dir, 'handoff-stale.sqlite');
  const stale = new Queue({ dbPath: stalePath, readyLimit: 2, leaseMs: 40 });
  stale.submitRoot({ id: 'stale-root', kind: 'fixture.tree' });
  const oldJob = stale.claim('old-worker')!;
  await sleep(70);
  assert.throws(
    () => stale.expand(oldJob, { children: [{ key: 'x', kind: 'fixture.ok' }] }),
    /LEASE_FENCED|expired|generation/,
    'lease-lost process cannot write/dispatch',
  );
  const reclaimed = stale.claim('new-worker')!;
  assert.equal(reclaimed.id, 'stale-root');
  assert.equal(reclaimed.fence, oldJob.fence + 1);
  const freshAck = stale.expand(reclaimed, {
    children: Array.from({ length: 9 }, (_, i) => ({ key: `s${i}`, kind: 'fixture.ok' })),
  });
  assert.equal(freshAck.totalChildren, 9);
  assert.equal(stale.expand(reclaimed, { children: [] }).totalChildren, 9);
  assertStatsWithinLimit(stale.stats(100_000));
  stale.close();
}

async function testHttpHandoffRetry(dir: string): Promise<void> {
  log('5/9 handoff retry over HTTP after a lost response packet');
  const dbPath = join(dir, 'http-handoff-retry.sqlite');
  const handle = await startQueueServer({ dbPath, port: 0, readyLimit: 2, leaseMs: 60_000 });
  try {
    const base = `http://127.0.0.1:${handle.port}`;
    const client = new HttpQueueClient(base);
    await client.submitRoot({ id: 'http-hr', kind: 'fixture.tree' });
    const job = await client.claim('http-retry-worker');
    assert.ok(job);

    const children = Array.from({ length: 25 }, (_, i) => ({
      key: `h${i}`,
      kind: 'fixture.ok',
      input: { i },
    }));
    const postExpand = async (lease: {
      workerId: string;
      leaseToken: string;
      fence: number;
    }): Promise<Response> =>
      fetch(`${base}/v1/checks/http-hr/expand`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ ...lease, children }),
      });

    const response1 = await postExpand(job);
    assert.equal(response1.status, 200);
    const ack1 = (await response1.json()) as { totalChildren: number };
    const response2 = await postExpand(job);
    assert.equal(response2.status, 200, 'retry after a lost response is accepted');
    const ack2 = (await response2.json()) as { totalChildren: number };
    assert.equal(ack2.totalChildren, 25);
    assert.deepEqual(ack2, ack1);

    // The client helper's own transport-retry path stays idempotent too.
    const ack3 = (await client.expand(job, children)) as { totalChildren: number };
    assert.equal(ack3.totalChildren, 25);

    const forbidden = await postExpand({ ...job, leaseToken: 'wrong-token' });
    assert.equal(forbidden.status, 409);
    const forbiddenBody = (await forbidden.json()) as { code?: string };
    assert.equal(forbiddenBody.code, 'LEASE_FENCED');

    const raw = new DatabaseSync(dbPath);
    raw.exec('PRAGMA busy_timeout=10000');
    const n = raw
      .prepare("SELECT COUNT(*) AS n FROM checks WHERE parent_id = 'http-hr'")
      .get() as { n: number | bigint };
    assert.equal(Number(n.n), 25, 'HTTP retry did not double-register the checklist');
    raw.close();

    const stats = await client.getStats();
    assertStatsWithinLimit(stats);
    log('handoff retry over HTTP: duplicate POSTs returned the same ack, 25 children registered once');
  } finally {
    handle.close();
  }
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
  log('6/9 one real worker process, cap=1, multi-layer failure-bearing tree');
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

async function testMultiProcessKillAndRecovery(dir: string): Promise<void> {
  log('7/9 three worker processes: multiprocess claiming');
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

  log('8/9 SIGKILL server and all workers; restart service and replacement workers');

  // Snapshot every acknowledged child result and the event horizon BEFORE the
  // kill so we can prove completed children are never claimed again and their
  // result rows are never replaced.
  const preKillRaw = new DatabaseSync(dbPath);
  preKillRaw.exec('PRAGMA busy_timeout=10000');
  const preKillResults = preKillRaw
    .prepare('SELECT child_id, completed_at FROM child_results')
    .all() as Array<{ child_id: string; completed_at: number | bigint }>;
  const horizonRow = preKillRaw
    .prepare('SELECT COALESCE(MAX(id), 0) AS n FROM worker_events')
    .get() as { n: number | bigint };
  const preKillEventHorizon = Number(horizonRow.n);
  preKillRaw.close();

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

  // Every child already terminal before the kill keeps its exact completed_at
  // and has no new claimed event after the pre-kill event horizon.
  const newClaimRows = rawFinal
    .prepare(
      `SELECT DISTINCT check_id FROM worker_events
       WHERE event = 'claimed' AND id > ? AND check_id IS NOT NULL`,
    )
    .all(preKillEventHorizon) as Array<{ check_id: string }>;
  const newClaimIds = new Set(newClaimRows.map((r) => r.check_id));
  let checkedPreKillResults = 0;
  const unchangedStmt = rawFinal.prepare(
    'SELECT completed_at FROM child_results WHERE child_id = ?',
  );
  for (const pre of preKillResults) {
    const after = unchangedStmt.get(pre.child_id) as { completed_at: number | bigint } | undefined;
    assert.ok(after, `pre-kill result ${pre.child_id} survives recovery`);
    assert.equal(
      Number(after.completed_at),
      Number(pre.completed_at),
      `pre-kill result ${pre.child_id} was not replaced`,
    );
    assert.ok(
      !newClaimIds.has(pre.child_id),
      `completed child ${pre.child_id} must not be claimed again after restart`,
    );
    checkedPreKillResults += 1;
  }
  assert.ok(checkedPreKillResults >= 6, 'the no-reclaim check covered real pre-kill results');

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

async function testWideHandoffThenKill(dir: string): Promise<void> {
  const width = 200;
  const limit = 4; // 50x buffer: a checklist far wider than the ready buffer.
  log(
    `9/9 宽清单强停场景: ${width} children vs readyLimit=${limit} (far wider than buffer); ` +
      'handoff confirmed, then 同时强停 service + original Worker (SIGKILL), restart and finish',
  );
  const dbPath = join(dir, 'wide-handoff-kill.sqlite');
  const server = await startSpawnedServer(dbPath, limit, 900);
  const client = new HttpQueueClient(`http://127.0.0.1:${server.port}`);

  const originalWorker = spawnCli(
    [
      'worker',
      '--http',
      `http://127.0.0.1:${server.port}`,
      '--fixtures',
      '--worker-id',
      'wide-original',
      '--poll-ms',
      '5',
      '--heartbeat-ms',
      '150',
      '--replenish-ms',
      '30',
    ],
    'wide-original-worker',
  );

  await client.submitRoot({
    id: 'wide-root',
    kind: 'fixture.tree',
    input: { depth: 1, breadth: width - 1, includeFailure: true, delayMs: 2, label: 'wide' },
  });

  // Confirm the durable full handoff: the expanded event carries the whole
  // requested checklist and progress already counts every logical row.
  await waitFor(
    async () => {
      const stats = await client.getStats();
      const event = stats.events.find(
        (e) => e.event === 'expanded' && e.checkId === 'wide-root',
      ) as { detail?: { children?: number } } | undefined;
      return event && event.detail?.children === width ? event : null;
    },
    { timeoutMs: 10_000, intervalMs: 40, label: 'wide checklist handoff confirmation' },
  );
  const handedOff = await client.getProgress('wide-root');
  assert.equal(handedOff.total, width + 1, 'every child row is registered at handoff');

  // Snapshot whatever (if anything) already finished in the tiny window
  // between handoff confirmation and the simultaneous SIGKILL. The kill is
  // deliberately immediate: this is the "confirmed handoff, then kill both"
  // scenario. The substantive completed-child no-reclaim check is section 8.
  const preKillRaw = new DatabaseSync(dbPath);
  preKillRaw.exec('PRAGMA busy_timeout=10000');
  const preKillResults = preKillRaw
    .prepare('SELECT child_id, completed_at FROM child_results')
    .all() as Array<{ child_id: string; completed_at: number | bigint }>;
  const horizonRow = preKillRaw
    .prepare('SELECT COALESCE(MAX(id), 0) AS n FROM worker_events')
    .get() as { n: number | bigint };
  const eventHorizon = Number(horizonRow.n);
  preKillRaw.close();

  // The named scenario: kill service AND the original worker at once, right
  // after the full checklist handoff was confirmed.
  await forceKill([server.process, originalWorker]);

  const killedQueue = new Queue({ dbPath, readyLimit: limit });
  const killedStats = killedQueue.stats(100_000);
  assertStatsWithinLimit(killedStats);
  assert.equal(killedStats.readyPeak, limit);
  const killedRaw = new DatabaseSync(dbPath);
  killedRaw.exec('PRAGMA busy_timeout=10000');
  const handoffReceipts = killedRaw
    .prepare("SELECT COUNT(*) AS n FROM operation_receipts WHERE op = 'expand' AND check_id = 'wide-root'")
    .get() as { n: number | bigint };
  assert.equal(Number(handoffReceipts.n), 1, 'handoff receipt survived SIGKILL');
  const killedChecks = killedRaw
    .prepare("SELECT COUNT(*) AS n FROM checks WHERE root_id = 'wide-root'")
    .get() as { n: number | bigint };
  assert.equal(Number(killedChecks.n), width + 1);
  killedRaw.close();
  killedQueue.close();

  // Restart service with replacement workers only; the original is gone.
  const restarted = await startSpawnedServer(dbPath, limit, 900);
  const recoveredClient = new HttpQueueClient(`http://127.0.0.1:${restarted.port}`);
  const recoveryWorkers: SpawnedProcess[] = [];
  for (const workerId of ['wide-r1', 'wide-r2']) {
    recoveryWorkers.push(
      spawnCli(
        [
          'worker',
          '--http',
          `http://127.0.0.1:${restarted.port}`,
          '--fixtures',
          '--worker-id',
          workerId,
          '--poll-ms',
          '8',
          '--heartbeat-ms',
          '200',
          '--replenish-ms',
          '30',
        ],
        workerId,
      ),
    );
  }

  const progress = await waitFor(
    async () => {
      const value = await recoveredClient.getProgress('wide-root').catch(() => null);
      return value && ['completed', 'failed'].includes(value.status) ? value : null;
    },
    { timeoutMs: 45_000, intervalMs: 150, label: 'wide root terminal after restart' },
  );
  assert.equal(progress.status, 'completed', JSON.stringify(progress));

  const expectedKeys = [
    ...Array.from({ length: width - 1 }, (_, i) => `b${i}`),
    'required-bad-child',
  ];
  const result = progress.result as {
    receivedChildCount: number;
    childKeys: string[];
    failedChildren: Array<{ key: string }>;
    children: Array<{ key: string; ordinal: number; status: string }>;
  };
  assert.equal(result.receivedChildCount, width);
  assert.deepEqual(result.childKeys, expectedKeys, 'parent receives results in checklist order');
  assert.deepEqual(
    result.children.map((c) => c.ordinal),
    Array.from({ length: width }, (_, i) => i),
    'ordinals cover the original checklist exactly and in order',
  );
  assert.equal(result.failedChildren.length, 1);
  assert.equal(result.failedChildren[0]?.key, 'required-bad-child');
  assert.equal(result.children.at(-1)?.status, 'failed', 'failure is delivered as a terminal result');
  assert.ok(
    result.children.slice(0, -1).every((c) => c.status === 'completed'),
    'all ordinary children completed',
  );

  await forceKill(recoveryWorkers);
  restarted.process.child.kill('SIGTERM');
  await restarted.process.exited.catch(() => undefined);

  const finalQueue = new Queue({ dbPath, readyLimit: limit });
  const stats = finalQueue.stats(100_000);
  assertStatsWithinLimit(stats);
  assert.equal(stats.readyPeak, limit, 'global ready peak never exceeded the cap');
  for (const op of ['expand', 'claim', 'replenish'] as const) {
    const maxRead = stats.listReadMaxByOp[op] ?? 0;
    assert.ok(maxRead <= limit, `${op} actual read max ${maxRead} exceeds ${limit}`);
    log(`wide scenario ${op}: maxActualRead(实际读取量)=${maxRead} <= limit ${limit}`);
  }
  assert.ok(stats.distinctClaimWorkers.includes('wide-original'));
  assert.ok(stats.distinctClaimWorkers.some((id) => id.startsWith('wide-r')));

  const rawFinal = new DatabaseSync(dbPath);
  rawFinal.exec('PRAGMA busy_timeout=10000');
  assert.equal(
    Number(
      (rawFinal.prepare('SELECT COUNT(*) AS n FROM checks').get() as { n: number | bigint }).n,
    ),
    width + 1,
  );
  assert.equal(
    Number(
      (rawFinal.prepare('SELECT COUNT(*) AS n FROM child_results').get() as { n: number | bigint })
        .n,
    ),
    width,
  );
  const dupes = rawFinal
    .prepare('SELECT child_id, COUNT(*) AS c FROM child_results GROUP BY child_id HAVING COUNT(*) > 1')
    .all();
  assert.equal(dupes.length, 0, 'one result row per logical child');

  const newClaims = rawFinal
    .prepare(
      `SELECT DISTINCT check_id FROM worker_events
        WHERE event = 'claimed' AND id > ? AND check_id IS NOT NULL`,
    )
    .all(eventHorizon) as Array<{ check_id: string }>;
  const newClaimIds = new Set(newClaims.map((r) => r.check_id));
  const unchangedStmt = rawFinal.prepare(
    'SELECT completed_at FROM child_results WHERE child_id = ?',
  );
  for (const pre of preKillResults) {
    const after = unchangedStmt.get(pre.child_id) as { completed_at: number | bigint } | undefined;
    assert.ok(after, `pre-kill completed child ${pre.child_id} still has its result`);
    assert.equal(
      Number(after.completed_at),
      Number(pre.completed_at),
      `completed child ${pre.child_id} result not replaced`,
    );
    assert.ok(
      !newClaimIds.has(pre.child_id),
      `completed child ${pre.child_id} must not be claimed again after restart`,
    );
  }
  rawFinal.close();
  finalQueue.close();
  log(
    `wide handoff recovery OK: ${width} ordered results incl. failure; ` +
      `${preKillResults.length} child result(s) survived the immediate kill and were never re-claimed; ` +
      `readyPeak=${stats.readyPeak}`,
  );
}

async function main(): Promise<void> {
  log(`using temporary database directory ${tempRoot}`);
  try {
    await testLeaseFencing(tempRoot);
    await testFairnessSecondClaim(tempRoot);
    await testLargeChecklistAndLimits(tempRoot);
    await testHandoffRetryDirect(tempRoot);
    await testHttpHandoffRetry(tempRoot);
    await testSingleProcessCapOneTree(tempRoot);
    await testMultiProcessKillAndRecovery(tempRoot);
    await testWideHandoffThenKill(tempRoot);

    const elapsedMs = Date.now() - startedAt;
    log(
      `PASS: all persistence, recovery, fairness, and limit checks passed in ${(elapsedMs / 1000).toFixed(1)}s — ` +
        `coverage: 实际读取量 bounded actual reads + global ready peak; 交接重试 handoff retry ` +
        `idempotent/no duplicate registration; stale-lease fencing; completed children not ` +
        `re-claimed; parent receives all ordered results incl. failures; far-wider-than-buffer ` +
        `confirmed-handoff SIGKILL(强停) of service and original worker then restart completes.`,
    );
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

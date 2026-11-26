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

/**
 * After a hard stop the killed worker can no longer heartbeat, but its lease
 * may still be unexpired. Expire every outstanding lease deterministically so
 * the replacement worker can reclaim immediately, without waiting out the
 * lease window. This mirrors what wall-clock expiry would do on its own.
 */
function expireAllLeases(dbPath: string): void {
  const raw = new DatabaseSync(dbPath);
  raw.exec('PRAGMA busy_timeout=10000');
  raw
    .prepare("UPDATE checks SET lease_expires_at = 0 WHERE status = 'leased'")
    .run();
  raw.close();
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
  log('1/8 lease expiry and stale fencing');
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
  log('2/8 cap=1 cross-tree fairness by second successful claim');
  const queue = new Queue({ dbPath: join(dir, 'fairness.sqlite'), readyLimit: 1 });

  queue.submitRoot({ id: 'tree-a', kind: 'fixture.tree' });
  const rootA = queue.claim('fair-worker');
  assert.ok(rootA);
  const fairList = Array.from({ length: 12 }, (_, index) => ({
    key: `a-${index}`,
    kind: 'fixture.ok',
  }));
  // cap=1: the wide checklist must be handed over one entry per segment,
  // and nothing is claimable until the finalizing segment seals it.
  for (let i = 0; i < fairList.length; i += 1) {
    const openAck = queue.expand(rootA, {
      children: [fairList[i]!],
      finalize: i === fairList.length - 1,
    });
    assert.equal(openAck.checklistRead, 1);
    if (i < fairList.length - 1) {
      assert.equal(openAck.sealed, false);
      assert.equal(queue.claim('fair-worker'), null, 'open handover children cannot be claimed');
    } else {
      assert.equal(openAck.sealed, true);
    }
  }
  // The stale lease was released when the handover sealed; the parent is now
  // waiting and the first child is the one ready entry.
  assert.throws(
    () => queue.expand(rootA, { children: [], finalize: true }),
    /LEASE_FENCED|generation|no active handover/,
  );

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
  log('3/8 large checklist, paged complete results, and read limits');
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
  assert.ok(
    (stats.listReadCountByOp.result_page ?? 0) >= 30,
    'result pages should be bounded',
  );
  // Wide 120-entry list at cap=4 needs exactly 30 sealed segments.
  const expandReads = stats.listReads.filter((s) => s.op === 'expand');
  assert.equal(expandReads.length, 30, '120 entries / cap 4 = 30 segment calls');
  assert.ok(expandReads.every((s) => s.readCount <= 4));
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
  const opList = Array.from({ length: 24 }, (_, i) => ({
    key: `c${i}`,
    kind: 'fixture.ok',
  }));
  for (let i = 0; i < opList.length; i += 3) {
    const segmentAck = opQueue.expand(opRoot, {
      children: opList.slice(i, i + 3),
      finalize: i + 3 >= opList.length,
    });
    assert.equal(segmentAck.checklistRead, 3);
  }

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

/**
 * Deterministic in-process coverage of the segmented handover protocol:
 * segment size cap, open-handover invisibility, fencing of lease losers,
 * cursor-based continuation and idempotent safe re-handover after a crash.
 */
async function testSegmentedHandoverProtocol(dir: string): Promise<void> {
  log('4/8 segmented handover protocol: cap, fencing, continue and safe re-handover');
  const dbPath = join(dir, 'segmented.sqlite');
  const queue = new Queue({ dbPath, readyLimit: 2, leaseMs: 8_000 });
  queue.submitRoot({ id: 'seg-root', kind: 'fixture.tree' });
  const lease = queue.claim('seg-worker')!;

  // An oversized segment is rejected before any write lands.
  assert.throws(
    () =>
      queue.expand(lease, {
        children: Array.from({ length: 3 }, (_, i) => ({ key: `x${i}`, kind: 'fixture.ok' })),
      }),
    /exceeds readyLimit 2/,
  );
  assert.equal(
    queue.getProgress('seg-root').total,
    1,
    'oversized segment must not persist any entry',
  );

  const list = Array.from({ length: 7 }, (_, i) => ({ key: `k${i}`, kind: 'fixture.ok' }));

  // Segment 0 (open): children stay staged and cannot be claimed.
  const s0 = queue.expand(lease, { children: list.slice(0, 2) });
  assert.equal(s0.sealed, false);
  assert.equal(s0.handoverOpen, true);
  assert.equal(s0.inserted, 2);
  assert.equal(s0.nextOrdinal, 2);
  assert.equal(queue.claim('seg-worker'), null, 'open handover entries must not be claimable');
  assert.equal(queue.replenish('probe').checklistRead, 0, 'replenish cannot see open handover');

  // Segment 1 (still open).
  const s1 = queue.expand(lease, { children: list.slice(2, 4) });
  assert.equal(s1.nextOrdinal, 4);
  assert.equal(queue.claim('seg-worker'), null);

  // A worker that lost its lease (wrong fence/generation) cannot publish any
  // segment, even though the handover is still open.
  const loser: typeof lease = { ...lease, fence: lease.fence + 999, leaseToken: 'stale-token' };
  assert.throws(
    () => queue.expand(loser, { children: list.slice(4, 6), finalize: true }),
    /LEASE_FENCED|generation/,
  );
  assert.equal(
    queue.claim('seg-worker'),
    null,
    'the still-active lease keeps the open handover parent leased',
  );

  // Continue from the durable cursor with the still-valid lease (segments 2,3).
  const s2 = queue.expand(lease, { children: list.slice(4, 6) });
  assert.equal(s2.nextOrdinal, 6);
  const s3 = queue.expand(lease, { children: list.slice(6, 7), finalize: true });
  assert.equal(s3.sealed, true);
  assert.equal(s3.totalChildren, 7);
  assert.equal(s3.admitted, 2, 'sealing admits one bounded segment into the ready buffer');

  // After sealing, children are claimable in ordinal order.
  const first = queue.claim('seg-worker')!;
  assert.equal(first.key, 'k0');
  queue.complete(first, { status: 'completed', result: 'r0' });
  const second = queue.claim('seg-worker')!;
  assert.equal(second.key, 'k1');
  queue.complete(second, { status: 'completed', result: 'r1' });

  const stats = queue.stats(100_000);
  assertStatsWithinLimit(stats);
  for (const read of stats.listReads.filter((r) => r.op === 'expand')) {
    assert.ok(read.readCount <= 2, `segment read ${read.readCount} exceeds cap 2`);
  }
  queue.close();

  // Safe re-handover: simulate a hard stop mid-handover then re-send entries.
  const reDbPath = join(dir, 'segmented-rehand.sqlite');
  const reQueue = new Queue({ dbPath: reDbPath, readyLimit: 2, leaseMs: 8_000 });
  reQueue.submitRoot({ id: 're-root', kind: 'fixture.tree' });
  const reLease = reQueue.claim('re-worker')!;
  reQueue.expand(reLease, { children: list.slice(0, 2) }); // open, 2 registered
  reQueue.expand(reLease, { children: list.slice(2, 4) }); // open, 4 registered

  // Re-hand the whole list with stable keys: the prefix is ignored idempotently.
  const reA = reQueue.expand(reLease, { children: list.slice(0, 2) });
  assert.equal(reA.ignored, 2);
  assert.equal(reA.inserted, 0);
  assert.equal(reA.nextOrdinal, 4);
  const reB = reQueue.expand(reLease, { children: list.slice(2, 4) });
  assert.equal(reB.ignored, 2);
  const reC = reQueue.expand(reLease, { children: list.slice(4, 6) });
  assert.equal(reC.inserted, 2);
  const reD = reQueue.expand(reLease, { children: list.slice(6, 7), finalize: true });
  assert.equal(reD.sealed, true);
  assert.equal(reD.totalChildren, 7);

  const raw = new DatabaseSync(reDbPath);
  raw.exec('PRAGMA busy_timeout=10000');
  const rows = raw
    .prepare('SELECT child_key, ordinal FROM checks WHERE parent_id = ? ORDER BY ordinal')
    .all('re-root') as Array<{ child_key: string; ordinal: number }>;
  assert.deepEqual(
    rows.map((r) => r.child_key),
    list.map((c) => c.key),
    're-handover preserves original ordinal order with no duplicates',
  );
  raw.close();
  reQueue.close();

  // An empty checklist sealed on the first call must not strand the parent.
  const emptyDbPath = join(dir, 'segmented-empty.sqlite');
  const emptyQueue = new Queue({ dbPath: emptyDbPath, readyLimit: 2, leaseMs: 8_000 });
  emptyQueue.submitRoot({ id: 'empty-root', kind: 'fixture.tree' });
  const emptyLease = emptyQueue.claim('empty-worker')!;
  const emptyAck = emptyQueue.expand(emptyLease, { children: [], finalize: true });
  assert.equal(emptyAck.sealed, true);
  assert.equal(emptyAck.totalChildren, 0);
  // With no children the parent becomes resumable and is admitted at once.
  const resumed = emptyQueue.claim('empty-worker')!;
  assert.equal(resumed.id, 'empty-root');
  assert.equal(resumed.resumable, true);
  assert.deepEqual(await emptyQueue.readAllChildResults('empty-root'), []);
  emptyQueue.complete(resumed, { status: 'completed', result: { receivedChildCount: 0 } });
  const emptyProgress = await emptyQueue.waitForTerminal('empty-root', { timeoutMs: 2_000 });
  assert.equal(emptyProgress.status, 'completed');
  assert.equal(emptyProgress.total, 1);
  emptyQueue.close();

  // Lease expiry mid-handover: a replacement generation reclaims the open
  // handover parent and continues from the durable cursor.
  const expireDbPath = join(dir, 'segmented-expire.sqlite');
  const expireQueue = new Queue({ dbPath: expireDbPath, readyLimit: 2, leaseMs: 40 });
  expireQueue.submitRoot({ id: 'exp-root', kind: 'fixture.tree' });
  const expFirst = expireQueue.claim('exp-w1')!;
  expireQueue.expand(expFirst, { children: list.slice(0, 2) }); // open, 2 registered
  await sleep(70); // let the lease expire while the handover is open
  assert.throws(
    () => expireQueue.expand(expFirst, { children: list.slice(2, 4) }),
    /LEASE_FENCED|expired/,
    'the expired generation can no longer publish a segment',
  );
  const expSecond = expireQueue.claim('exp-w2')!;
  assert.equal(expSecond.id, 'exp-root');
  assert.equal(expSecond.handoverOpen, true, 'reclaimed parent still reports an open handover');
  assert.equal(expSecond.handoverNextOrdinal, 2, 'cursor points at the first unregistered ordinal');
  // Continue with the new generation from ordinal 2, then seal.
  const c2 = expireQueue.expand(expSecond, { children: list.slice(2, 4) });
  assert.equal(c2.nextOrdinal, 4);
  const c3 = expireQueue.expand(expSecond, { children: list.slice(4, 6) });
  assert.equal(c3.nextOrdinal, 6);
  const c4 = expireQueue.expand(expSecond, { children: list.slice(6), finalize: true });
  assert.equal(c4.sealed, true);
  assert.equal(c4.totalChildren, 7);
  const expireRaw = new DatabaseSync(expireDbPath);
  expireRaw.exec('PRAGMA busy_timeout=10000');
  const ordered = expireRaw
    .prepare('SELECT child_key FROM checks WHERE parent_id = ? ORDER BY ordinal')
    .all('exp-root') as Array<{ child_key: string }>;
  assert.deepEqual(
    ordered.map((r) => r.child_key),
    list.map((c) => c.key),
  );
  expireRaw.close();
  expireQueue.close();
}

/**
 * Wide local checklist with a single real worker process at readyLimit=1.
 * The worker is SIGKILLed before the handover, while it is open, and after it
 * sealed; each restart must resume safely. A second phase restarts again to
 * prove undispatched entries survive and completed results are reused exactly
 * once. Covers the recursive case: every internal tree node hands over its
 * own segmented checklist.
 */
async function testWideChecklistHardStop(dir: string): Promise<void> {
  log('5/8 single worker, cap=1, wide recursive checklist, hard stops around handover');
  const dbPath = join(dir, 'wide-hardstop.sqlite');
  const limit = 1;
  const breadth = 9;
  const queue = new Queue({ dbPath, readyLimit: limit, leaseMs: 60_000 });
  queue.submitRoot({
    id: 'wide-root',
    kind: 'fixture.tree',
    input: { depth: 2, breadth, delayMs: 0, startDelayMs: 120, label: 'wide' },
  });

  const workerArgs = (workerId: string): string[] => [
    'worker',
    '--db',
    dbPath,
    '--fixtures',
    '--worker-id',
    workerId,
    '--ready-limit',
    String(limit),
    '--lease-ms',
    '60000',
    '--heartbeat-ms',
    '1000',
    '--poll-ms',
    '5',
    '--replenish-ms',
    '15',
    // Widen the open-handover window so SIGKILL lands between segments.
    '--handover-segment-delay-ms',
    '30',
  ];

  // Phase A: kill before the first segment is sent.
  const before = spawnCli(workerArgs('wide-w1'), 'wide-before');
  await waitFor(
    () => before.events.find((e) => e.type === 'worker-start' && e.workerId === 'wide-w1'),
    { timeoutMs: 5_000, label: 'phase A worker start' },
  );
  await sleep(60); // inside startDelayMs, no handover yet
  await forceKill([before]);
  expireAllLeases(dbPath);
  const rawA = new DatabaseSync(dbPath);
  rawA.exec('PRAGMA busy_timeout=10000');
  assert.equal(
    (rawA.prepare("SELECT COUNT(*) AS n FROM checks WHERE handover_open = 1").get() as { n: number }).n,
    0,
    'no open handover exists before the first segment',
  );
  rawA.close();

  // Phase B: restart; kill while the root handover is open.
  const during = spawnCli(workerArgs('wide-w2'), 'wide-during');
  await waitFor(
    () => during.events.find((e) => e.type === 'handover-segment' && e.sealed === false),
    { timeoutMs: 10_000, label: 'phase B open handover segment' },
  );
  await forceKill([during]);
  expireAllLeases(dbPath);
  const rawB = new DatabaseSync(dbPath);
  rawB.exec('PRAGMA busy_timeout=10000');
  const openRow = rawB
    .prepare("SELECT id FROM checks WHERE handover_open = 1 AND status = 'leased' LIMIT 1")
    .get() as { id: string } | undefined;
  assert.ok(openRow, 'open handover must survive SIGKILL with the parent leased');
  const openChildCount = Number(
    (rawB.prepare('SELECT COUNT(*) AS n FROM checks WHERE parent_id = ?').get(openRow.id) as { n: number }).n,
  );
  assert.ok(openChildCount > 0 && openChildCount < breadth, 'only a prefix is registered');
  const openInvisible = Number(
    (
      rawB.prepare("SELECT COUNT(*) AS n FROM checks WHERE status = 'ready'").get() as {
        n: number;
      }
    ).n,
  );
  assert.equal(openInvisible, 0, 'unsealed children must not be ready after a hard stop');
  rawB.close();

  // Phase C: restart and let the root handover seal, then kill again.
  const after = spawnCli(workerArgs('wide-w3'), 'wide-after');
  await waitFor(
    () => after.events.find((e) => e.type === 'handover-resume'),
    { timeoutMs: 10_000, label: 'phase C resumes the open handover' },
  );
  await waitFor(
    () => after.events.find((e) => e.type === 'handover-sealed' && e.checkId === 'wide-root'),
    { timeoutMs: 10_000, label: 'phase C root handover sealed' },
  );
  await forceKill([after]);
  expireAllLeases(dbPath);

  // Phase D: fresh worker drains the whole tree to completion.
  const finisher = spawnCli(workerArgs('wide-w4'), 'wide-finish');
  const progress = await queue.waitForTerminal('wide-root', { timeoutMs: 40_000, pollMs: 40 });
  assert.equal(progress.status, 'completed');
  const result = progress.result as { receivedChildCount: number; childKeys: string[] };
  assert.equal(result.receivedChildCount, breadth);
  assert.deepEqual(
    result.childKeys,
    Array.from({ length: breadth }, (_, i) => `b${i}`),
  );
  finisher.child.kill('SIGTERM');
  await finisher.exited.catch(() => undefined);

  const expectedChecks = 1 + breadth + breadth * breadth; // depth=2 tree
  const rawFinal = new DatabaseSync(dbPath);
  rawFinal.exec('PRAGMA busy_timeout=10000');
  const checkCount = Number(
    (rawFinal.prepare('SELECT COUNT(*) AS n FROM checks').get() as { n: number }).n,
  );
  assert.equal(checkCount, expectedChecks, 'every logical child exists exactly once');
  const duplicateKeys = rawFinal
    .prepare(
      `SELECT parent_id, child_key, COUNT(*) AS c
       FROM checks GROUP BY parent_id, child_key HAVING COUNT(*) > 1`,
    )
    .all();
  assert.equal(duplicateKeys.length, 0, 'no logical child was registered twice');
  const duplicateResults = rawFinal
    .prepare('SELECT child_id, COUNT(*) AS c FROM child_results GROUP BY child_id HAVING COUNT(*) > 1')
    .all();
  assert.equal(duplicateResults.length, 0, 'each logical child has exactly one result row');
  const openLeft = Number(
    (rawFinal.prepare('SELECT COUNT(*) AS n FROM checks WHERE handover_open = 1').get() as { n: number }).n,
  );
  assert.equal(openLeft, 0, 'no handover remains open after completion');
  rawFinal.close();

  const stats = queue.stats(100_000);
  assert.equal(stats.readyPeak, 1);
  assertStatsWithinLimit(stats);
  for (const read of stats.listReads) {
    assert.ok(read.readCount <= limit, `actual read ${read.readCount} > cap ${limit}`);
  }
  assert.ok(
    stats.listReads.some((r) => r.op === 'expand' && r.trigger === 'segment'),
    'segmented registration reads are recorded by actual access count',
  );
  const segmentEvents = stats.events.filter((e) => e.event === 'handover-segment');
  assert.ok(segmentEvents.length >= breadth, 'each single-entry cap=1 segment is recorded');
  log(
    `hard-stop stats: checks=${checkCount}, segments=${segmentEvents.length}, ` +
      `maxReads=${JSON.stringify(stats.listReadMaxByOp)}`,
  );
  queue.close();
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
  log('6/8 one real worker process, cap=1, multi-layer failure-bearing tree');
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
  log('7/8 three worker processes: multiprocess claiming');
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

  log('8/8 SIGKILL server and all workers; restart service and replacement workers');
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
    await testSegmentedHandoverProtocol(tempRoot);
    await testWideChecklistHardStop(tempRoot);
    await testSingleProcessCapOneTree(tempRoot);
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

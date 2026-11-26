import { randomUUID } from 'node:crypto';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import {
  ChildResult,
  ChildResultPage,
  ClaimedCheck,
  CompletionInput,
  ExpansionAck,
  Lease,
  LeaseFencedError,
  ListReadStat,
  NotFoundError,
  Progress,
  QueueStats,
  ValidationError,
  serializeError,
  type CheckDescriptor,
  type CheckStatus,
  type SerializedError,
} from './types.js';

export interface QueueOptions {
  dbPath: string;
  readyLimit?: number;
  leaseMs?: number;
  now?: () => number;
}

interface CheckRow {
  id: string;
  root_id: string;
  parent_id: string | null;
  child_key: string | null;
  ordinal: number | bigint | null;
  kind: string;
  input_json: string;
  status: CheckStatus;
  resumable: number | bigint;
  attempt: number | bigint;
  fence: number | bigint;
  worker_id: string | null;
  lease_token: string | null;
  lease_expires_at: number | bigint | null;
  result_status: string | null;
  result_json: string | null;
  error_json: string | null;
  submitted_seq: number | bigint | null;
  admission_seq: number | bigint | null;
  ready_since: number | bigint | null;
  created_at: number | bigint;
  updated_at: number | bigint;
}

interface Budget {
  used: number;
  max: number;
}

function num(value: number | bigint | null | undefined): number {
  if (value === null || value === undefined) return 0;
  return typeof value === 'bigint' ? Number(value) : value;
}

function parseJson<T>(text: string | null, fallback: T): T {
  if (text === null || text === undefined || text === '') return fallback;
  return JSON.parse(text) as T;
}

/**
 * Durable SQLite queue.
 *
 * Every state transition is committed with BEGIN IMMEDIATE. The ready buffer
 * never exceeds readyLimit. Checklist scans are explicitly LIMITed and their
 * row counts are recorded in list_read_stats.
 */
export class Queue {
  readonly dbPath: string;
  readyLimit: number;
  leaseMs: number;
  private readonly readyLimitArgument?: number;
  private readonly leaseMsArgument?: number;
  private readonly now: () => number;
  private readonly db: DatabaseSync;

  constructor(options: QueueOptions) {
    this.dbPath = options.dbPath;
    this.readyLimitArgument =
      options.readyLimit === undefined ? undefined : Math.max(1, Math.floor(options.readyLimit));
    this.leaseMsArgument =
      options.leaseMs === undefined ? undefined : Math.max(50, Math.floor(options.leaseMs));
    this.readyLimit = this.readyLimitArgument ?? 4;
    this.leaseMs = this.leaseMsArgument ?? 30_000;
    this.now = options.now ?? Date.now;
    if (options.dbPath !== ':memory:') {
      mkdirSync(dirname(options.dbPath), { recursive: true });
    }
    this.db = new DatabaseSync(this.dbPath);
    this.db.exec('PRAGMA journal_mode=WAL');
    this.db.exec('PRAGMA synchronous=FULL');
    this.db.exec('PRAGMA busy_timeout=10000');
    this.db.exec('PRAGMA foreign_keys=ON');
    this.migrate();
  }

  close(): void {
    this.db.close();
  }

  private migrate(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS queue_meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS checks (
        id TEXT PRIMARY KEY,
        root_id TEXT NOT NULL,
        parent_id TEXT REFERENCES checks(id) ON DELETE CASCADE,
        child_key TEXT,
        ordinal INTEGER,
        kind TEXT NOT NULL,
        input_json TEXT NOT NULL,
        status TEXT NOT NULL,
        resumable INTEGER NOT NULL DEFAULT 0,
        attempt INTEGER NOT NULL DEFAULT 0,
        fence INTEGER NOT NULL DEFAULT 0,
        worker_id TEXT,
        lease_token TEXT,
        lease_expires_at INTEGER,
        result_status TEXT,
        result_json TEXT,
        error_json TEXT,
        submitted_seq INTEGER,
        admission_seq INTEGER,
        ready_since INTEGER,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        UNIQUE(parent_id, child_key)
      );

      CREATE TABLE IF NOT EXISTS child_results (
        child_id TEXT PRIMARY KEY REFERENCES checks(id) ON DELETE CASCADE,
        parent_id TEXT NOT NULL REFERENCES checks(id) ON DELETE CASCADE,
        child_key TEXT NOT NULL,
        ordinal INTEGER NOT NULL,
        status TEXT NOT NULL,
        result_json TEXT,
        error_json TEXT,
        completed_at INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS list_read_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        at INTEGER NOT NULL,
        op TEXT NOT NULL,
        trigger_name TEXT,
        parent_id TEXT,
        read_count INTEGER NOT NULL,
        ready_before INTEGER NOT NULL,
        ready_after INTEGER NOT NULL,
        ready_limit INTEGER NOT NULL,
        detail_json TEXT
      );

      CREATE TABLE IF NOT EXISTS worker_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        at INTEGER NOT NULL,
        event TEXT NOT NULL,
        worker_id TEXT,
        check_id TEXT,
        root_id TEXT,
        detail_json TEXT
      );

      -- Durable idempotency receipts. A response packet can be lost after the
      -- transaction commits: a retry carrying the SAME lease generation replays
      -- the original response without writing or dispatching anything again,
      -- while a different/stale generation is still fenced.
      CREATE TABLE IF NOT EXISTS operation_receipts (
        check_id TEXT NOT NULL,
        op TEXT NOT NULL,
        fence INTEGER NOT NULL,
        worker_id TEXT NOT NULL,
        lease_token TEXT NOT NULL,
        response_json TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        PRIMARY KEY (check_id, op, fence)
      );

      CREATE INDEX IF NOT EXISTS idx_checks_candidate
        ON checks(status, ready_since, lease_expires_at);
      CREATE INDEX IF NOT EXISTS idx_checks_parent_ord
        ON checks(parent_id, ordinal);
      CREATE INDEX IF NOT EXISTS idx_checks_root
        ON checks(root_id);
      CREATE INDEX IF NOT EXISTS idx_checks_staged_children
        ON checks(status, parent_id, ordinal)
        WHERE status = 'staged';
      CREATE INDEX IF NOT EXISTS idx_child_results_parent
        ON child_results(parent_id, ordinal);
      CREATE INDEX IF NOT EXISTS idx_events_worker
        ON worker_events(worker_id, event);
    `);

    const existingLimit = this.getMeta('ready_limit');
    if (existingLimit === null) {
      this.setMeta('ready_limit', String(this.readyLimit));
    } else {
      const storedLimit = Number(existingLimit);
      if (
        this.readyLimitArgument !== undefined &&
        storedLimit !== this.readyLimitArgument
      ) {
        // The on-disk scheduling invariant belongs to the queue. An explicit
        // mismatch is an operator error; omitted options adopt stored values.
        throw new ValidationError(
          `readyLimit mismatch: database uses ${existingLimit}, process requested ${this.readyLimitArgument}`,
        );
      }
      this.readyLimit = storedLimit;
    }

    const existingLeaseMs = this.getMeta('lease_ms');
    if (existingLeaseMs === null) {
      this.setMeta('lease_ms', String(this.leaseMs));
    } else if (this.leaseMsArgument === undefined) {
      this.leaseMs = Number(existingLeaseMs);
    } else if (Number(existingLeaseMs) !== this.leaseMsArgument) {
      // Lease duration can reasonably differ per claimant because each claim
      // stamps its own expiry, so the explicit process value remains in use.
      this.leaseMs = this.leaseMsArgument;
    }
    if (this.getMeta('ready_peak') === null) {
      this.setMeta('ready_peak', '0');
    }
  }

  private getMeta(key: string): string | null {
    const row = this.db.prepare('SELECT value FROM queue_meta WHERE key = ?').get(key) as
      | { value: string }
      | undefined;
    return row?.value ?? null;
  }

  private setMeta(key: string, value: string): void {
    this.db
      .prepare(
        `INSERT INTO queue_meta(key, value) VALUES(?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
      )
      .run(key, value);
  }

  private withTx<T>(fn: () => T): T {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const result = fn();
      this.db.exec('COMMIT');
      return result;
    } catch (error) {
      try {
        this.db.exec('ROLLBACK');
      } catch {
        // The original error is the useful one.
      }
      throw error;
    }
  }

  private nextMetaSequence(key: string): number {
    const current = Number(this.getMeta(key) ?? '0');
    const next = current + 1;
    this.setMeta(key, String(next));
    return next;
  }

  private recordEvent(
    event: string,
    workerId: string | null,
    check?: Pick<CheckRow, 'id' | 'root_id'> | null,
    detail?: unknown,
  ): void {
    this.db
      .prepare(
        `INSERT INTO worker_events(at, event, worker_id, check_id, root_id, detail_json)
         VALUES (?, ?, ?, ?, ?, ?)`,
      )
      .run(
        this.now(),
        event,
        workerId,
        check?.id ?? null,
        check?.root_id ?? null,
        detail === undefined ? null : JSON.stringify(detail),
      );
  }

  private recordListRead(input: {
    op: ListReadStat['op'];
    trigger?: string | null;
    parentId?: string | null;
    readCount: number;
    readyBefore: number;
    detail?: unknown;
  }): void {
    const readyAfter = this.readyCount();
    this.db
      .prepare(
        `INSERT INTO list_read_stats(
           at, op, trigger_name, parent_id, read_count,
           ready_before, ready_after, ready_limit, detail_json
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        this.now(),
        input.op,
        input.trigger ?? null,
        input.parentId ?? null,
        input.readCount,
        input.readyBefore,
        readyAfter,
        this.readyLimit,
        input.detail === undefined ? null : JSON.stringify(input.detail),
      );
    this.updatePeak(readyAfter);
  }

  private readyCount(): number {
    const row = this.db.prepare("SELECT COUNT(*) AS n FROM checks WHERE status = 'ready'").get() as {
      n: number | bigint;
    };
    return num(row.n);
  }

  private updatePeak(current = this.readyCount()): void {
    const peak = Number(this.getMeta('ready_peak') ?? '0');
    if (current > peak) this.setMeta('ready_peak', String(current));
  }

  private getRow(id: string): CheckRow | undefined {
    return this.db.prepare('SELECT * FROM checks WHERE id = ?').get(id) as CheckRow | undefined;
  }

  private requireRow(id: string): CheckRow {
    const row = this.getRow(id);
    if (!row) throw new NotFoundError(`check not found: ${id}`);
    return row;
  }

  private toClaimed(row: CheckRow): ClaimedCheck {
    return {
      id: row.id,
      rootId: row.root_id,
      parentId: row.parent_id,
      key: row.child_key,
      ordinal: row.ordinal === null ? null : num(row.ordinal),
      kind: row.kind,
      input: parseJson(row.input_json, null),
      status: row.status,
      resumable: num(row.resumable) === 1,
      attempt: num(row.attempt),
      fence: num(row.fence),
      workerId: row.worker_id ?? '',
      leaseToken: row.lease_token ?? '',
      leaseExpiresAt: num(row.lease_expires_at),
      claimedAt: this.now(),
    };
  }

  /** Submit a root check. It is durably staged even when the ready buffer is full. */
  submitRoot(descriptor: CheckDescriptor & { id?: string }): {
    id: string;
    status: CheckStatus;
  } {
    const id = descriptor.id ?? randomUUID();
    const existing = this.getRow(id);
    if (existing) return { id: existing.id, status: existing.status };
    if (!descriptor.kind) throw new ValidationError('kind is required');

    const now = this.now();
    return this.withTx(() => {
      const doubleCheck = this.getRow(id);
      if (doubleCheck) {
        return { id: doubleCheck.id, status: doubleCheck.status };
      }
      const seq = this.nextMetaSequence('root_seq');
      const ready = this.readyCount();
      const status: CheckStatus = ready < this.readyLimit ? 'ready' : 'staged';
      this.db
        .prepare(
          `INSERT INTO checks(
             id, root_id, parent_id, child_key, ordinal, kind, input_json,
             status, resumable, submitted_seq, ready_since, created_at, updated_at
           ) VALUES (?, ?, NULL, NULL, NULL, ?, ?, ?, 0, ?, ?, ?, ?)`,
        )
        .run(
          id,
          id,
          descriptor.kind,
          JSON.stringify(descriptor.input ?? null),
          status,
          seq,
          status === 'ready' ? now : null,
          now,
          now,
        );
      this.updatePeak();
      this.recordEvent('submitted', null, { id, root_id: id }, { status, kind: descriptor.kind });
      return { id, status: status as 'ready' | 'staged' };
    });
  }

  /**
   * Admit staged roots and resumable parents. This performs no checklist read.
   * Caller is already inside a transaction.
   */
  private admitPriorityItems(): string[] {
    const admitted: string[] = [];
    const now = this.now();

    let free = this.readyLimit - this.readyCount();
    if (free > 0) {
      const roots = this.db
        .prepare(
          `SELECT id FROM checks
           WHERE status = 'staged' AND parent_id IS NULL
           ORDER BY submitted_seq ASC
           LIMIT ?`,
        )
        .all(free) as Array<{ id: string }>;
      for (const root of roots) {
        this.db
          .prepare(
            `UPDATE checks SET status = 'ready', ready_since = ?, updated_at = ?
             WHERE id = ? AND status = 'staged'`,
          )
          .run(now, now, root.id);
        admitted.push(root.id);
      }
    }

    free = this.readyLimit - this.readyCount();
    if (free > 0) {
      const resumes = this.db
        .prepare(
          `SELECT id FROM checks
           WHERE status = 'staged' AND resumable = 1
           ORDER BY admission_seq ASC
           LIMIT ?`,
        )
        .all(free) as Array<{ id: string }>;
      for (const item of resumes) {
        this.db
          .prepare(
            `UPDATE checks SET status = 'ready', ready_since = ?, updated_at = ?
             WHERE id = ? AND status = 'staged' AND resumable = 1`,
          )
          .run(now, now, item.id);
        admitted.push(item.id);
      }
    }

    if (admitted.length > 0) this.updatePeak();
    return admitted;
  }

  /**
   * Read at most budget.remaining checklist rows and admit them.
   * The budget is shared by all reads in one external operation.
   */
  private admitChecklistChildren(budget: Budget): { readCount: number; ids: string[] } {
    const free = this.readyLimit - this.readyCount();
    const pageLimit = Math.max(0, Math.min(free, budget.max - budget.used));
    if (pageLimit <= 0) return { readCount: 0, ids: [] };

    const rows = this.db
      .prepare(
        `SELECT c.id
         FROM checks c
         JOIN checks r ON r.id = c.root_id
         WHERE c.status = 'staged'
           AND c.parent_id IS NOT NULL
           AND c.resumable = 0
         ORDER BY r.submitted_seq ASC,
                  c.created_at ASC,
                  c.parent_id ASC,
                  c.ordinal ASC
         LIMIT ?`,
      )
      .all(pageLimit) as Array<{ id: string }>;

    const now = this.now();
    const ids: string[] = [];
    for (const row of rows) {
      const result = this.db
        .prepare(
          `UPDATE checks SET status = 'ready', ready_since = ?, updated_at = ?
           WHERE id = ? AND status = 'staged'`,
        )
        .run(now, now, row.id);
      if (num(result.changes) === 1) ids.push(row.id);
    }
    budget.used += rows.length;
    if (ids.length > 0) this.updatePeak();
    return { readCount: rows.length, ids };
  }

  /** Public/background buffer replenishment. Reads at most readyLimit entries. */
  replenish(trigger = 'background'): {
    admitted: string[];
    checklistRead: number;
    readyCurrent: number;
  } {
    return this.withTx(() => {
      const readyBefore = this.readyCount();
      const admittedPriority = this.admitPriorityItems();
      const budget: Budget = { used: 0, max: this.readyLimit };
      const children = this.admitChecklistChildren(budget);
      const admitted = [...admittedPriority, ...children.ids];
      this.recordListRead({
        op: 'replenish',
        trigger,
        readCount: budget.used,
        readyBefore,
        detail: { admitted },
      });
      return {
        admitted,
        checklistRead: budget.used,
        readyCurrent: this.readyCount(),
      };
    });
  }

  /**
   * Atomically claim a ready check or reclaim an expired lease.
   * A successful claim performs at most readyLimit checklist reads while
   * refilling one or more vacant buffer slots.
   */
  claim(workerId: string, options: { leaseMs?: number } = {}): ClaimedCheck | null {
    if (!workerId) throw new ValidationError('workerId is required');
    const leaseMs = options.leaseMs ?? this.leaseMs;

    return this.withTx(() => {
      const readyBefore = this.readyCount();
      const budget: Budget = { used: 0, max: this.readyLimit };

      let row = this.selectCandidate(this.now());
      let preAdmitted: string[] = [];
      if (!row) {
        preAdmitted = this.admitPriorityItems();
        const page = this.admitChecklistChildren(budget);
        preAdmitted.push(...page.ids);
        row = this.selectCandidate(this.now());
      }

      if (!row) {
        if (budget.used > 0) {
          this.recordListRead({
            op: 'claim',
            trigger: 'claim-empty',
            readCount: budget.used,
            readyBefore,
            detail: { admitted: preAdmitted },
          });
        }
        return null;
      }

      const now = this.now();
      const token = randomUUID();
      const expiresAt = now + leaseMs;
      const reclaimed = row.status === 'leased';
      const updated = this.db
        .prepare(
          `UPDATE checks
             SET status = 'leased',
                 worker_id = ?,
                 lease_token = ?,
                 fence = fence + 1,
                 attempt = attempt + 1,
                 lease_expires_at = ?,
                 updated_at = ?
           WHERE id = ?
             AND (status = 'ready' OR (status = 'leased' AND lease_expires_at <= ?))`,
        )
        .run(workerId, token, expiresAt, now, row.id, now);

      if (num(updated.changes) !== 1) {
        // Lost a concurrent claim race. SQLite write transactions are serial,
        // so this is defensive.
        return null;
      }

      const claimed = this.requireRow(row.id);
      this.recordEvent('claimed', workerId, claimed, {
        fence: num(claimed.fence),
        attempt: num(claimed.attempt),
        reclaimed,
        previousStatus: row.status,
      });

      // Refill the vacancy created by claiming a ready item, and admit any
      // priority work. The shared budget keeps this claim under the read cap.
      const postPriority = this.admitPriorityItems();
      const postChildren = this.admitChecklistChildren(budget);
      this.recordListRead({
        op: 'claim',
        trigger: reclaimed ? 'reclaim' : 'claim',
        parentId: null,
        readCount: budget.used,
        readyBefore,
        detail: {
          checkId: claimed.id,
          admitted: [...preAdmitted, ...postPriority, ...postChildren.ids],
        },
      });

      return this.toClaimed(claimed);
    });
  }

  private selectCandidate(now: number): CheckRow | undefined {
    return this.db
      .prepare(
        `SELECT * FROM checks
         WHERE status = 'ready'
            OR (status = 'leased' AND lease_expires_at <= ?)
         ORDER BY
           CASE WHEN status = 'ready' THEN COALESCE(ready_since, created_at)
                ELSE lease_expires_at END ASC,
           rowid ASC
         LIMIT 1`,
      )
      .get(now) as CheckRow | undefined;
  }

  private assertActiveLease(lease: Lease): CheckRow {
    const row = this.requireRow(lease.id);
    if (
      row.status !== 'leased' ||
      row.worker_id !== lease.workerId ||
      row.lease_token !== lease.leaseToken ||
      num(row.fence) !== lease.fence
    ) {
      throw new LeaseFencedError(`lease generation mismatch for ${lease.id}`);
    }
    if (row.lease_expires_at === null || num(row.lease_expires_at) <= this.now()) {
      throw new LeaseFencedError(`lease expired for ${lease.id}`);
    }
    return row;
  }

  /**
   * Return the stored response when this lease generation already committed
   * the operation. A receipt owned by a different worker/token is a stale
   * generation and must be fenced rather than replayed.
   */
  private findReceipt<T>(op: string, lease: Lease): T | null {
    const checkId = lease.id;
    const row = this.db
      .prepare(
        `SELECT worker_id, lease_token, response_json
           FROM operation_receipts
          WHERE check_id = ? AND op = ? AND fence = ?`,
      )
      .get(checkId, op, lease.fence) as
      | { worker_id: string; lease_token: string; response_json: string }
      | undefined;
    if (!row) return null;
    if (row.worker_id !== lease.workerId || row.lease_token !== lease.leaseToken) {
      throw new LeaseFencedError(
        `${op} retry for ${checkId} does not match the committed lease generation`,
      );
    }
    return parseJson<T>(row.response_json, null as T);
  }

  private writeReceipt(checkId: string, op: string, lease: Lease, response: unknown): void {
    this.db
      .prepare(
        `INSERT INTO operation_receipts(
           check_id, op, fence, worker_id, lease_token, response_json, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        checkId,
        op,
        lease.fence,
        lease.workerId,
        lease.leaseToken,
        JSON.stringify(response),
        this.now(),
      );
  }

  /** Extend an active lease. Expired or fenced callers are rejected. */
  heartbeat(lease: Lease, options: { leaseMs?: number } = {}): ClaimedCheck {
    const leaseMs = options.leaseMs ?? this.leaseMs;
    return this.withTx(() => {
      this.assertActiveLease(lease);
      const expiresAt = this.now() + leaseMs;
      this.db
        .prepare('UPDATE checks SET lease_expires_at = ?, updated_at = ? WHERE id = ?')
        .run(expiresAt, this.now(), lease.id);
      const row = this.requireRow(lease.id);
      this.recordEvent('heartbeat', lease.workerId, row, { fence: lease.fence });
      return this.toClaimed(row);
    });
  }

  /**
   * Durably decode/register the ENTIRE child checklist in one transaction,
   * then hand the parent off to `waiting` and admit one bounded ready segment.
   *
   * The call is idempotent per lease generation: if the response was lost,
   * retrying with the same workerId/leaseToken/fence replays the original ack
   * without inserting children again, writing state, or dispatching more items.
   * A stale or different generation is rejected by the receipt/lease checks.
   */
  expand(lease: Lease, input: { children: CheckDescriptor[] }): ExpansionAck {
    const children = input.children ?? [];
    if (!Array.isArray(children)) throw new ValidationError('children must be an array');
    children.forEach((child, index) => {
      if (!child || typeof child !== 'object') {
        throw new ValidationError(`child at ordinal ${index} must be a descriptor`);
      }
      if (!child.kind) throw new ValidationError(`child at ordinal ${index} lacks kind`);
    });

    return this.withTx(() => {
      // A same-generation retry is a read-only replay of the durable receipt:
      // it inserts nothing and dispatches nothing, so even a caller whose lease
      // has since elapsed cannot write or schedule through the retry.
      const replay = this.findReceipt<ExpansionAck>('expand', lease);
      if (replay) return replay;

      const parent = this.assertActiveLease(lease);
      const readyBefore = this.readyCount();
      const now = this.now();

      const insert = this.db.prepare(
        `INSERT OR IGNORE INTO checks(
           id, root_id, parent_id, child_key, ordinal, kind, input_json,
           status, resumable, created_at, updated_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, 'staged', 0, ?, ?)`,
      );

      // One-pass full registration: every descriptor is durably written before
      // the parent releases its execution slot or any child is dispatched.
      children.forEach((child, index) => {
        const key = child.key ?? String(index);
        const id = child.id ?? randomUUID();
        insert.run(
          id,
          parent.root_id,
          parent.id,
          key,
          index,
          child.kind,
          JSON.stringify(child.input ?? null),
          now,
          now,
        );
      });

      // Complete the handoff: only now does the parent stop holding its slot.
      this.db
        .prepare(
          `UPDATE checks
             SET status = 'waiting',
                 lease_token = NULL,
                 lease_expires_at = NULL,
                 updated_at = ?
           WHERE id = ?`,
        )
        .run(now, parent.id);

      // An empty checklist has no child completion to trigger resume, so hand
      // the parent straight back to the resumable queue.
      this.maybeResumeParent(parent.id, now);

      // If capacity exists, priority work should enter first. Under the
      // scheduler invariant a pending priority item implies a full buffer, so
      // this normally changes nothing here.
      this.admitPriorityItems();

      const free = this.readyLimit - this.readyCount();
      const pageLimit = Math.max(0, Math.min(this.readyLimit, free));
      let readRows: Array<{ id: string }> = [];
      if (pageLimit > 0) {
        readRows = this.db
          .prepare(
            `SELECT id FROM checks
             WHERE parent_id = ? AND status = 'staged'
             ORDER BY ordinal ASC
             LIMIT ?`,
          )
          .all(parent.id, pageLimit) as Array<{ id: string }>;
        for (const row of readRows) {
          this.db
            .prepare(
              `UPDATE checks SET status = 'ready', ready_since = ?, updated_at = ?
               WHERE id = ? AND status = 'staged'`,
            )
            .run(now, now, row.id);
        }
      }

      // An immediately-resumed empty checklist may now fit a free slot.
      this.admitPriorityItems();

      this.updatePeak();
      const totalChildrenRow = this.db
        .prepare('SELECT COUNT(*) AS n FROM checks WHERE parent_id = ?')
        .get(parent.id) as { n: number | bigint };
      const totalChildren = num(totalChildrenRow.n);

      this.recordListRead({
        op: 'expand',
        trigger: 'register',
        parentId: parent.id,
        readCount: readRows.length,
        readyBefore,
        detail: { requested: children.length, totalChildren },
      });
      this.recordEvent('expanded', lease.workerId, parent, {
        children: children.length,
        admitted: readRows.length,
      });

      const ack: ExpansionAck = {
        id: parent.id,
        totalChildren,
        admitted: readRows.length,
        checklistRead: readRows.length,
        readLimit: this.readyLimit,
      };
      this.writeReceipt(parent.id, 'expand', lease, ack);
      return ack;
    });
  }

  /**
   * Commit a direct result. The conditional lease update fences stale workers.
   * A terminal child inserts at most one child_results row.
   */
  complete(lease: Lease, outcome: CompletionInput): { id: string; status: CheckStatus } {
    const status = outcome.status === 'failed' ? 'failed' : 'completed';
    const errorJson =
      status === 'failed'
        ? JSON.stringify(outcome.error ? serializeError(outcome.error) : { message: 'check failed' })
        : null;
    const resultJson = outcome.result === undefined ? null : JSON.stringify(outcome.result);

    return this.withTx(() => {
      const row = this.assertActiveLease(lease);
      const now = this.now();

      const updated = this.db
        .prepare(
          `UPDATE checks
             SET status = ?,
                 result_status = ?,
                 result_json = ?,
                 error_json = ?,
                 lease_token = NULL,
                 lease_expires_at = NULL,
                 updated_at = ?
           WHERE id = ?
             AND status = 'leased'
             AND worker_id = ?
             AND lease_token = ?
             AND fence = ?
             AND lease_expires_at > ?`,
        )
        .run(
          status,
          status,
          resultJson,
          errorJson,
          now,
          row.id,
          lease.workerId,
          lease.leaseToken,
          lease.fence,
          now,
        );

      if (num(updated.changes) !== 1) {
        throw new LeaseFencedError(`could not complete fenced check ${row.id}`);
      }

      if (row.parent_id) {
        const ordinal = row.ordinal === null ? 0 : num(row.ordinal);
        this.db
          .prepare(
            `INSERT OR IGNORE INTO child_results(
               child_id, parent_id, child_key, ordinal, status,
               result_json, error_json, completed_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
          )
          .run(
            row.id,
            row.parent_id,
            row.child_key ?? String(ordinal),
            ordinal,
            status,
            resultJson,
            errorJson,
            now,
          );
      }

      this.recordEvent('completed', lease.workerId, row, { status });
      this.maybeResumeParent(row.parent_id, now);

      // Completion may have made a slot available indirectly or left capacity
      // below the cap; replenish within the same durable transaction.
      const readyBefore = this.readyCount();
      const budget: Budget = { used: 0, max: this.readyLimit };
      const priority = this.admitPriorityItems();
      const children = this.admitChecklistChildren(budget);
      if (budget.used > 0 || priority.length > 0 || children.ids.length > 0) {
        this.recordListRead({
          op: 'replenish',
          trigger: 'complete',
          parentId: row.parent_id,
          readCount: budget.used,
          readyBefore,
          detail: { admitted: [...priority, ...children.ids] },
        });
      }

      return { id: row.id, status };
    });
  }

  private maybeResumeParent(parentId: string | null, now: number): void {
    if (!parentId) return;
    const parent = this.getRow(parentId);
    if (!parent || parent.status !== 'waiting') return;

    const pending = this.db
      .prepare(
        `SELECT COUNT(*) AS n FROM checks
         WHERE parent_id = ?
           AND status NOT IN ('completed', 'failed')`,
      )
      .get(parentId) as { n: number | bigint };

    if (num(pending.n) !== 0) return;

    const admissionSeq = this.nextMetaSequence('admission_seq');
    this.db
      .prepare(
        `UPDATE checks
           SET status = 'staged',
               resumable = 1,
               admission_seq = ?,
               lease_token = NULL,
               lease_expires_at = NULL,
               updated_at = ?
         WHERE id = ? AND status = 'waiting'`,
      )
      .run(admissionSeq, now, parentId);
    this.recordEvent('children-complete', null, parent, { admissionSeq });
  }

  /**
   * Read one bounded page of the complete child-result set. The worker calls
   * this repeatedly before resuming a parent; the claim operation itself does
   * not perform an unbounded result scan.
   */
  readChildResultsPage(
    parentId: string,
    afterOrdinal = -1,
    pageSize = this.readyLimit,
  ): ChildResultPage {
    const size = Math.max(1, Math.min(this.readyLimit, Math.floor(pageSize || this.readyLimit)));
    const rows = this.db
      .prepare(
        `SELECT * FROM child_results
         WHERE parent_id = ? AND ordinal > ?
         ORDER BY ordinal ASC
         LIMIT ?`,
      )
      .all(parentId, afterOrdinal, size) as Array<{
      child_id: string;
      child_key: string;
      ordinal: number | bigint;
      status: 'completed' | 'failed';
      result_json: string | null;
      error_json: string | null;
      completed_at: number | bigint;
    }>;

    const items: ChildResult[] = rows.map((r) => ({
      childId: r.child_id,
      key: r.child_key,
      ordinal: num(r.ordinal),
      status: r.status,
      result: parseJson(r.result_json, undefined),
      error: parseJson<SerializedError | undefined>(r.error_json, undefined),
      completedAt: num(r.completed_at),
    }));

    const last = items.at(-1)?.ordinal ?? afterOrdinal;
    this.withTx(() => {
      const readyBefore = this.readyCount();
      this.recordListRead({
        op: 'result_page',
        trigger: 'resume',
        parentId,
        readCount: items.length,
        readyBefore,
      });
    });

    return {
      items,
      nextAfterOrdinal: items.length === size ? last : null,
    };
  }

  async readAllChildResults(parentId: string): Promise<ChildResult[]> {
    const all: ChildResult[] = [];
    let after: number | null = -1;
    while (after !== null) {
      const page = this.readChildResultsPage(parentId, after, this.readyLimit);
      all.push(...page.items);
      after = page.nextAfterOrdinal;
    }
    return all;
  }

  getCheck(id: string): (ClaimedCheck & { rawStatus: CheckStatus }) | null {
    const row = this.getRow(id);
    if (!row) return null;
    return { ...this.toClaimed(row), rawStatus: row.status };
  }

  getProgress(rootId: string): Progress {
    const root = this.requireRow(rootId);
    const rows = this.db
      .prepare(
        `WITH RECURSIVE tree(id) AS (
           SELECT id FROM checks WHERE id = ?
           UNION ALL
           SELECT c.id
           FROM checks c
           JOIN tree t ON c.parent_id = t.id
         )
         SELECT c.status AS status, COUNT(*) AS n
         FROM checks c
         JOIN tree t ON t.id = c.id
         GROUP BY c.status`,
      )
      .all(rootId) as Array<{ status: CheckStatus; n: number | bigint }>;

    const counts: Record<CheckStatus, number> = {
      staged: 0,
      ready: 0,
      leased: 0,
      waiting: 0,
      completed: 0,
      failed: 0,
    };
    for (const row of rows) counts[row.status] = num(row.n);
    const total = Object.values(counts).reduce((a, b) => a + b, 0);
    const terminal = counts.completed + counts.failed;
    const result = parseJson(root.result_json, undefined);
    const error = parseJson<SerializedError | undefined>(root.error_json, undefined);

    return {
      rootId,
      status: root.status,
      total,
      ...counts,
      terminal,
      percent: total === 0 ? 0 : Math.round((terminal / total) * 100),
      ...(root.status === 'completed' ? { result } : {}),
      ...(root.status === 'failed' ? { error } : {}),
    };
  }

  getResult(rootId: string): { status: CheckStatus; result?: unknown; error?: SerializedError } {
    const row = this.requireRow(rootId);
    return {
      status: row.status,
      ...(row.result_json !== null ? { result: parseJson(row.result_json, undefined) } : {}),
      ...(row.error_json !== null
        ? { error: parseJson<SerializedError>(row.error_json, { name: 'Error', message: '' }) }
        : {}),
    };
  }

  stats(limitEvents = 500): QueueStats {
    const statusRows = this.db
      .prepare('SELECT status, COUNT(*) AS n FROM checks GROUP BY status')
      .all() as Array<{ status: CheckStatus; n: number | bigint }>;
    const statusCounts: Record<CheckStatus, number> = {
      staged: 0,
      ready: 0,
      leased: 0,
      waiting: 0,
      completed: 0,
      failed: 0,
    };
    for (const row of statusRows) statusCounts[row.status] = num(row.n);

    const readRows = this.db
      .prepare('SELECT * FROM list_read_stats ORDER BY id ASC LIMIT ?')
      .all(limitEvents) as Array<Record<string, unknown>>;
    const listReads: ListReadStat[] = readRows.map((r) => ({
      id: num(r.id as number | bigint),
      at: num(r.at as number | bigint),
      op: r.op as ListReadStat['op'],
      trigger: (r.trigger_name as string | null) ?? null,
      parentId: (r.parent_id as string | null) ?? null,
      readCount: num(r.read_count as number | bigint),
      readyBefore: num(r.ready_before as number | bigint),
      readyAfter: num(r.ready_after as number | bigint),
      readyLimit: num(r.ready_limit as number | bigint),
      ...(r.detail_json ? { detail: JSON.parse(r.detail_json as string) } : {}),
    }));

    const maxByOp: Record<string, number> = {};
    const countByOp: Record<string, number> = {};
    for (const stat of listReads) {
      maxByOp[stat.op] = Math.max(maxByOp[stat.op] ?? 0, stat.readCount);
      countByOp[stat.op] = (countByOp[stat.op] ?? 0) + 1;
    }

    const claimCountRow = this.db
      .prepare("SELECT COUNT(*) AS n FROM worker_events WHERE event = 'claimed'")
      .get() as { n: number | bigint };
    const workers = this.db
      .prepare(
        "SELECT DISTINCT worker_id FROM worker_events WHERE event = 'claimed' AND worker_id IS NOT NULL ORDER BY worker_id",
      )
      .all() as Array<{ worker_id: string }>;

    const eventRows = this.db
      .prepare('SELECT * FROM worker_events ORDER BY id DESC LIMIT ?')
      .all(limitEvents) as Array<Record<string, unknown>>;
    const events = eventRows
      .map((r) => ({
        id: num(r.id as number | bigint),
        at: num(r.at as number | bigint),
        event: r.event as string,
        workerId: (r.worker_id as string | null) ?? null,
        checkId: (r.check_id as string | null) ?? null,
        rootId: (r.root_id as string | null) ?? null,
        ...(r.detail_json ? { detail: JSON.parse(r.detail_json as string) } : {}),
      }))
      .reverse();

    return {
      dbPath: this.dbPath,
      readyLimit: this.readyLimit,
      leaseMs: this.leaseMs,
      readyCurrent: this.readyCount(),
      readyPeak: Number(this.getMeta('ready_peak') ?? '0'),
      statusCounts,
      listReads,
      listReadMaxByOp: maxByOp,
      listReadCountByOp: countByOp,
      claims: num(claimCountRow.n),
      distinctClaimWorkers: workers.map((w) => w.worker_id),
      events,
    };
  }

  /** Test/helper: wait for a root to become terminal. */
  async waitForTerminal(
    rootId: string,
    options: { timeoutMs?: number; pollMs?: number } = {},
  ): Promise<Progress> {
    const timeoutMs = options.timeoutMs ?? 30_000;
    const pollMs = options.pollMs ?? 50;
    const started = this.now();
    while (this.now() - started < timeoutMs) {
      const progress = this.getProgress(rootId);
      if (progress.status === 'completed' || progress.status === 'failed') return progress;
      await new Promise((resolve) => setTimeout(resolve, pollMs));
    }
    throw new Error(`timed out waiting for root ${rootId}`);
  }
}

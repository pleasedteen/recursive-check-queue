export type CheckStatus =
  | 'staged'
  | 'ready'
  | 'leased'
  | 'waiting'
  | 'completed'
  | 'failed';

export type TerminalStatus = 'completed' | 'failed';

export interface CheckDescriptor {
  /** Stable within the parent checklist. Defaults to the ordinal. */
  key?: string;
  /** Optional caller-supplied globally unique id. */
  id?: string;
  kind: string;
  input?: unknown;
}

export interface Lease {
  id: string;
  workerId: string;
  leaseToken: string;
  fence: number;
}

export type ExpansionStateName = 'none' | 'open' | 'confirmed';

export interface ExpansionStatus {
  /** none: no checklist handed over yet. open: segments registered, not confirmed. */
  state: ExpansionStateName;
  /** Number of descriptors durably registered so far. */
  registered: number;
}

export interface ClaimedCheck {
  id: string;
  rootId: string;
  parentId: string | null;
  key: string | null;
  ordinal: number | null;
  kind: string;
  input: unknown;
  status: CheckStatus;
  resumable: boolean;
  attempt: number;
  fence: number;
  workerId: string;
  leaseToken: string;
  leaseExpiresAt: number;
  claimedAt: number;
  /** Checklist handover state, useful when resuming an interrupted registration. */
  expansion: ExpansionStatus;
}

export interface SerializedError {
  name: string;
  message: string;
  stack?: string;
  code?: string;
  [key: string]: unknown;
}

export interface ChildResult {
  childId: string;
  key: string;
  ordinal: number;
  kind?: string;
  status: TerminalStatus;
  result?: unknown;
  error?: SerializedError;
  completedAt: number;
}

export interface ChildResultPage {
  items: ChildResult[];
  nextAfterOrdinal: number | null;
}

export interface CompletionInput {
  status: TerminalStatus;
  result?: unknown;
  error?: SerializedError | Error | unknown;
}

export interface ExpansionAck {
  id: string;
  totalChildren: number;
  admitted: number;
  checklistRead: number;
  readLimit: number;
}

export interface AppendAck {
  id: string;
  state: ExpansionStateName;
  registered: number;
  added: number;
  decoded: number;
  readLimit: number;
}

export interface ListReadStat {
  id: number;
  at: number;
  op: 'expand' | 'register' | 'claim' | 'replenish' | 'result_page';
  trigger: string | null;
  parentId: string | null;
  readCount: number;
  readyBefore: number;
  readyAfter: number;
  readyLimit: number;
  detail?: unknown;
}

export interface QueueStats {
  dbPath: string;
  readyLimit: number;
  leaseMs: number;
  readyCurrent: number;
  readyPeak: number;
  statusCounts: Record<CheckStatus, number>;
  listReads: ListReadStat[];
  listReadMaxByOp: Record<string, number>;
  listReadCountByOp: Record<string, number>;
  claims: number;
  distinctClaimWorkers: string[];
  events: Array<{
    id: number;
    at: number;
    event: string;
    workerId: string | null;
    checkId: string | null;
    rootId: string | null;
    detail?: unknown;
  }>;
}

export interface Progress {
  rootId: string;
  status: CheckStatus;
  total: number;
  staged: number;
  ready: number;
  leased: number;
  waiting: number;
  completed: number;
  failed: number;
  terminal: number;
  percent: number;
  result?: unknown;
  error?: SerializedError;
}

export class QueueError extends Error {
  code = 'QUEUE_ERROR';
  statusCode = 500;
  constructor(message: string) {
    super(message);
    this.name = 'QueueError';
  }
}

export class NotFoundError extends QueueError {
  code = 'NOT_FOUND';
  statusCode = 404;
  constructor(message: string) {
    super(message);
    this.name = 'NotFoundError';
  }
}

export class LeaseFencedError extends QueueError {
  code = 'LEASE_FENCED';
  statusCode = 409;
  constructor(message = 'lease is no longer held by this worker generation') {
    super(message);
    this.name = 'LeaseFencedError';
  }
}

export class ValidationError extends QueueError {
  code = 'VALIDATION';
  statusCode = 400;
  constructor(message: string) {
    super(message);
    this.name = 'ValidationError';
  }
}

export function serializeError(error: unknown): SerializedError {
  if (error instanceof Error) {
    const out: SerializedError = {
      name: error.name || 'Error',
      message: error.message || String(error),
    };
    if (error.stack) out.stack = error.stack;
    const anyError = error as Error & { code?: unknown };
    if (anyError.code !== undefined) out.code = String(anyError.code);
    return out;
  }
  if (typeof error === 'object' && error !== null) {
    const maybe = error as { name?: unknown; message?: unknown; stack?: unknown; code?: unknown };
    return {
      name: typeof maybe.name === 'string' ? maybe.name : 'Error',
      message: typeof maybe.message === 'string' ? maybe.message : JSON.stringify(error),
      ...(typeof maybe.stack === 'string' ? { stack: maybe.stack } : {}),
      ...(maybe.code !== undefined ? { code: String(maybe.code) } : {}),
    };
  }
  return { name: 'Error', message: String(error) };
}

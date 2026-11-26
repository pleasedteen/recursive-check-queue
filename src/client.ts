import type {
  ChildResult,
  ClaimedCheck,
  ExpansionAck,
  Lease,
  Progress,
  QueueStats,
} from './types.js';
import type { WorkerBackend } from './worker.js';
import type { CheckDescriptor } from './types.js';

export interface HttpQueueClientOptions {
  baseUrl: string;
  workerId?: string;
}

export class HttpQueueClient implements WorkerBackend {
  private readonly baseUrl: string;

  constructor(options: HttpQueueClientOptions | string) {
    this.baseUrl = (
      typeof options === 'string' ? options : options.baseUrl
    ).replace(/\/$/, '');
  }

  private async request<T>(path: string, init?: RequestInit): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      ...init,
      headers: {
        ...(init?.body ? { 'content-type': 'application/json' } : {}),
        ...(init?.headers ?? {}),
      },
    });

    if (response.status === 204) return null as T;
    const text = await response.text();
    const body = text ? (JSON.parse(text) as unknown) : null;

    if (!response.ok) {
      const data = (body ?? {}) as { code?: string; error?: string; message?: string };
      const error = new Error(data.error ?? data.message ?? `HTTP ${response.status}`) as Error & {
        code?: string;
        statusCode?: number;
      };
      error.code = data.code;
      error.statusCode = response.status;
      throw error;
    }
    return body as T;
  }

  private post<T>(path: string, body: unknown): Promise<T> {
    return this.request<T>(path, { method: 'POST', body: JSON.stringify(body) });
  }

  async submitRoot(descriptor: CheckDescriptor & { id?: string }): Promise<{ id: string; status: string }> {
    return this.post('/v1/checks', descriptor);
  }

  async getProgress(id: string): Promise<Progress> {
    return this.request<Progress>(`/v1/checks/${encodeURIComponent(id)}`);
  }

  async getStats(): Promise<QueueStats> {
    return this.request<QueueStats>('/v1/stats');
  }

  async claim(workerId: string): Promise<ClaimedCheck | null> {
    return this.post<ClaimedCheck | null>('/v1/claims', { workerId });
  }

  async heartbeat(lease: Lease): Promise<unknown> {
    return this.post(`/v1/checks/${encodeURIComponent(lease.id)}/heartbeat`, {
      workerId: lease.workerId,
      leaseToken: lease.leaseToken,
      fence: lease.fence,
    });
  }

  async expand(
    lease: Lease,
    children: CheckDescriptor[],
    options?: { finalize?: boolean },
  ): Promise<ExpansionAck> {
    return this.post(`/v1/checks/${encodeURIComponent(lease.id)}/expand`, {
      workerId: lease.workerId,
      leaseToken: lease.leaseToken,
      fence: lease.fence,
      children,
      ...(options?.finalize ? { finalize: true } : {}),
    });
  }

  async complete(
    lease: Lease,
    outcome: { status: 'completed' | 'failed'; result?: unknown; error?: unknown },
  ): Promise<unknown> {
    return this.post(`/v1/checks/${encodeURIComponent(lease.id)}/complete`, {
      workerId: lease.workerId,
      leaseToken: lease.leaseToken,
      fence: lease.fence,
      ...outcome,
    });
  }

  async readChildResultsPage(
    parentId: string,
    afterOrdinal = -1,
    pageSize?: number,
  ): Promise<{ items: ChildResult[]; nextAfterOrdinal: number | null }> {
    const params = new URLSearchParams({ afterOrdinal: String(afterOrdinal) });
    if (pageSize !== undefined) params.set('pageSize', String(pageSize));
    return this.request(`/v1/checks/${encodeURIComponent(parentId)}/child-results?${params}`);
  }

  async readAllChildResults(parentId: string): Promise<ChildResult[]> {
    const all: ChildResult[] = [];
    let after: number | null = -1;
    while (after !== null) {
      const page = await this.readChildResultsPage(parentId, after);
      all.push(...page.items);
      after = page.nextAfterOrdinal;
    }
    return all;
  }

  async replenish(trigger = 'http'): Promise<unknown> {
    return this.post('/v1/replenish', { trigger });
  }
}

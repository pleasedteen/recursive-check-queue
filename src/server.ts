import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { AddressInfo } from 'node:net';
import { Queue, type QueueOptions } from './queue.js';
import { QueueError, ValidationError, type Lease } from './types.js';

export interface QueueServerOptions extends QueueOptions {
  port?: number;
  host?: string;
  replenishMs?: number;
}

export interface StartedQueueServer {
  server: Server;
  queue: Queue;
  port: number;
  close: () => void;
}

async function readJson(request: IncomingMessage): Promise<any> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += buffer.length;
    if (size > 100 * 1024 * 1024) throw new QueueError('request body too large');
    chunks.push(buffer);
  }
  if (chunks.length === 0) return {};
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8')) as unknown;
  } catch {
    throw new ValidationError('request body must be valid JSON');
  }
}

function sendJson(response: ServerResponse, status: number, body: unknown): void {
  const json = JSON.stringify(body);
  response.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(json),
  });
  response.end(json);
}

function leaseFromBody(id: string, body: any): Lease {
  return {
    id,
    workerId: String(body.workerId ?? ''),
    leaseToken: String(body.leaseToken ?? ''),
    fence: Number(body.fence ?? 0),
  };
}

export function startQueueServer(options: QueueServerOptions): Promise<StartedQueueServer> {
  const queue = new Queue(options);
  const host = options.host ?? '127.0.0.1';
  const requestedPort = options.port ?? 4318;

  const replenishTimer =
    options.replenishMs === null
      ? null
      : setInterval(() => {
          try {
            queue.replenish('background-server');
          } catch {
            // Request/result paths also replenish; keep the HTTP process alive.
          }
        }, options.replenishMs ?? 250);
  replenishTimer?.unref?.();

  const server = createServer(async (request, response) => {
    try {
      const url = new URL(request.url ?? '/', `http://${host}`);
      const path = url.pathname;
      const method = request.method ?? 'GET';

      if (method === 'GET' && path === '/health') {
        sendJson(response, 200, { ok: true });
        return;
      }

      if (method === 'GET' && path === '/v1/stats') {
        const requestedLimit = Number(url.searchParams.get('limit') ?? '10000');
        const limit = Number.isFinite(requestedLimit)
          ? Math.min(100_000, Math.max(1, Math.floor(requestedLimit)))
          : 10_000;
        sendJson(response, 200, queue.stats(limit));
        return;
      }

      if (method === 'POST' && path === '/v1/checks') {
        const body = await readJson(request);
        const result = queue.submitRoot(body);
        sendJson(response, 202, result);
        return;
      }

      if (method === 'POST' && path === '/v1/claims') {
        const body = await readJson(request);
        const claimed = queue.claim(String(body.workerId ?? ''), {
          leaseMs: body.leaseMs === undefined ? undefined : Number(body.leaseMs),
        });
        if (!claimed) {
          response.writeHead(204);
          response.end();
          return;
        }
        sendJson(response, 200, claimed);
        return;
      }

      const checkMatch = /^\/v1\/checks\/([^/]+)(?:\/(heartbeat|expand|complete|child-results))?$/.exec(path);
      if (checkMatch) {
        const id = decodeURIComponent(checkMatch[1]!);
        const action = checkMatch[2];

        if (!action && method === 'GET') {
          sendJson(response, 200, queue.getProgress(id));
          return;
        }

        if (action === 'child-results' && method === 'GET') {
          const afterOrdinal = Number(url.searchParams.get('afterOrdinal') ?? '-1');
          const pageSizeParam = url.searchParams.get('pageSize');
          const page = queue.readChildResultsPage(
            id,
            Number.isFinite(afterOrdinal) ? afterOrdinal : -1,
            pageSizeParam === null ? queue.readyLimit : Number(pageSizeParam),
          );
          sendJson(response, 200, page);
          return;
        }

        const body = await readJson(request);
        const lease = leaseFromBody(id, body);

        if (action === 'heartbeat' && method === 'POST') {
          sendJson(response, 200, queue.heartbeat(lease));
          return;
        }
        if (action === 'expand' && method === 'POST') {
          sendJson(response, 200, queue.expand(lease, { children: body.children ?? [] }));
          return;
        }
        if (action === 'complete' && method === 'POST') {
          sendJson(
            response,
            200,
            queue.complete(lease, {
              status: body.status === 'failed' ? 'failed' : 'completed',
              result: body.result,
              error: body.error,
            }),
          );
          return;
        }
      }

      if (method === 'POST' && path === '/v1/replenish') {
        const body = await readJson(request).catch(() => ({}));
        sendJson(response, 200, queue.replenish(String(body.trigger ?? 'http')));
        return;
      }

      sendJson(response, 404, { code: 'NOT_FOUND', error: `no route for ${method} ${path}` });
    } catch (error) {
      const anyError = error as QueueError & { statusCode?: number };
      const status = anyError.statusCode ?? 500;
      if (status >= 500) console.error('[queue-server]', error);
      sendJson(response, status, {
        code: anyError.code ?? 'INTERNAL',
        error: error instanceof Error ? error.message : String(error),
      });
    }
  });

  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(requestedPort, host, () => {
      const address = server.address() as AddressInfo | null;
      const port = address?.port ?? requestedPort;
      resolve({
        server,
        queue,
        port,
        close: () => {
          if (replenishTimer) clearInterval(replenishTimer);
          server.closeAllConnections?.();
          server.close();
          queue.close();
        },
      });
    });
  });
}

#!/usr/bin/env node
import { resolve } from 'node:path';
import { HttpQueueClient } from './client.js';
import { createFixtureHandlers } from './fixtures.js';
import { Queue } from './queue.js';
import { startQueueServer } from './server.js';
import { DirectQueueBackend, Worker } from './worker.js';

interface ParsedArgs {
  command: string;
  positionals: string[];
  values: Map<string, string | boolean>;
}

function parseArgs(argv: string[]): ParsedArgs {
  const [command = 'help', ...rest] = argv;
  const values = new Map<string, string | boolean>();
  const positionals: string[] = [];

  for (let i = 0; i < rest.length; i += 1) {
    const token = rest[i]!;
    if (token.startsWith('--')) {
      const raw = token.slice(2);
      const eq = raw.indexOf('=');
      if (eq >= 0) {
        values.set(raw.slice(0, eq), raw.slice(eq + 1));
      } else {
        const next = rest[i + 1];
        if (next === undefined || next.startsWith('--')) {
          values.set(raw, true);
        } else {
          values.set(raw, next);
          i += 1;
        }
      }
    } else {
      positionals.push(token);
    }
  }
  return { command, positionals, values };
}

function stringArg(args: ParsedArgs, name: string, fallback?: string): string | undefined {
  const value = args.values.get(name);
  if (value === undefined) return fallback;
  return String(value);
}

function numberArg(args: ParsedArgs, name: string, fallback: number): number {
  const value = args.values.get(name);
  if (value === undefined || value === true) return fallback;
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function boolArg(args: ParsedArgs, name: string, fallback = false): boolean {
  const value = args.values.get(name);
  if (value === undefined) return fallback;
  return value === true || value === 'true' || value === '1';
}

function maybeNumberArg(args: ParsedArgs, name: string): number | undefined {
  if (!args.values.has(name)) return undefined;
  return numberArg(args, name, Number.NaN);
}

function printJson(value: unknown): void {
  process.stdout.write(`${JSON.stringify(value)}\n`);
}

function help(): void {
  process.stdout.write(`plugin-check persistent queue

Commands:
  serve      --db ./queue.sqlite [--port 4318] [--ready-limit 4] [--lease-ms 30000]
  worker     (--db ./queue.sqlite | --http http://127.0.0.1:4318) --fixtures
             [--worker-id ID] [--lease-ms 5000] [--heartbeat-ms 1500] [--poll-ms 50]
  submit     (--db PATH | --http URL) --kind fixture.tree [--input '{}'] [--id ID]
  inspect    (--db PATH | --http URL) --id ROOT_ID
  stats      (--db PATH | --http URL)

One worker process always has at most one active check execution slot.
`);
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));

  if (args.command === 'help' || args.command === '--help' || args.command === '-h') {
    help();
    return;
  }

  if (args.command === 'serve') {
    const dbPath = resolve(stringArg(args, 'db', './queue.sqlite')!);
    const port = numberArg(args, 'port', 4318);
    const readyLimit = maybeNumberArg(args, 'ready-limit');
    const leaseMs = maybeNumberArg(args, 'lease-ms');
    const started = await startQueueServer({
      dbPath,
      port,
      ...(readyLimit === undefined ? {} : { readyLimit }),
      ...(leaseMs === undefined ? {} : { leaseMs }),
      replenishMs: numberArg(args, 'replenish-ms', 250),
    });
    printJson({ type: 'listening', port: started.port, host: '127.0.0.1', db: dbPath });

    const shutdown = (signal: string): void => {
      printJson({ type: 'server-shutdown', signal });
      started.close();
      process.exit(0);
    };
    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));
    return;
  }

  if (args.command === 'worker') {
    const httpUrl = stringArg(args, 'http');
    const dbPath = stringArg(args, 'db');
    const workerId = stringArg(args, 'worker-id');
    const pollMs = numberArg(args, 'poll-ms', 40);
    const heartbeatMs = numberArg(args, 'heartbeat-ms', 1_500);
    const replenishMs = numberArg(args, 'replenish-ms', 250);
    const handoverSegmentDelayMs = numberArg(args, 'handover-segment-delay-ms', 0);

    let backend;
    let directQueue: Queue | null = null;
    if (httpUrl) {
      backend = new HttpQueueClient(httpUrl);
    } else {
      const resolvedDb = resolve(dbPath ?? './queue.sqlite');
      const readyLimit = maybeNumberArg(args, 'ready-limit');
      const leaseMs = maybeNumberArg(args, 'lease-ms');
      directQueue = new Queue({
        dbPath: resolvedDb,
        ...(readyLimit === undefined ? {} : { readyLimit }),
        ...(leaseMs === undefined ? {} : { leaseMs }),
      });
      backend = new DirectQueueBackend(directQueue);
    }

    if (!boolArg(args, 'fixtures', false)) {
      throw new Error('refusing to start worker without --fixtures in this local check build');
    }
    const handlers = createFixtureHandlers();
    const worker = new Worker(backend, handlers, {
      workerId,
      pollMs,
      heartbeatMs,
      replenishMs: boolArg(args, 'background-replenish', true) ? replenishMs : null,
      handoverSegmentDelayMs,
      onEvent: (event) => printJson(event),
    });

    let shuttingDown = false;
    let forceExitTimer: NodeJS.Timeout | undefined;
    const shutdown = (signal: string): void => {
      if (shuttingDown) return;
      shuttingDown = true;
      printJson({ type: 'worker-shutdown', signal, workerId: worker.workerId });
      worker.stop();
      forceExitTimer = setTimeout(() => process.exit(0), 2_000);
    };
    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));
    await worker.run();
    if (forceExitTimer) clearTimeout(forceExitTimer);
    directQueue?.close();
    return;
  }

  if (args.command === 'submit') {
    const kind = stringArg(args, 'kind');
    if (!kind) throw new Error('--kind is required');
    const id = stringArg(args, 'id');
    let input: unknown;
    const inputRaw = stringArg(args, 'input');
    const inputFile = stringArg(args, 'input-file');
    if (inputFile) {
      const { readFileSync } = await import('node:fs');
      input = JSON.parse(readFileSync(resolve(inputFile), 'utf8'));
    } else if (inputRaw) {
      input = JSON.parse(inputRaw);
    }

    const httpUrl = stringArg(args, 'http');
    if (httpUrl) {
      const client = new HttpQueueClient(httpUrl);
      printJson(await client.submitRoot({ id, kind, input }));
      return;
    }
    const queue = new Queue({
      dbPath: resolve(stringArg(args, 'db', './queue.sqlite')!),
      ...(args.values.has('ready-limit')
        ? { readyLimit: numberArg(args, 'ready-limit', 4) }
        : {}),
    });
    try {
      printJson(queue.submitRoot({ id, kind, input }));
    } finally {
      queue.close();
    }
    return;
  }

  if (args.command === 'inspect' || args.command === 'progress') {
    const id = stringArg(args, 'id');
    if (!id) throw new Error('--id is required');
    const httpUrl = stringArg(args, 'http');
    if (httpUrl) {
      printJson(await new HttpQueueClient(httpUrl).getProgress(id));
      return;
    }
    const queue = new Queue({ dbPath: resolve(stringArg(args, 'db', './queue.sqlite')!) });
    try {
      printJson(queue.getProgress(id));
    } finally {
      queue.close();
    }
    return;
  }

  if (args.command === 'stats') {
    const httpUrl = stringArg(args, 'http');
    if (httpUrl) {
      printJson(await new HttpQueueClient(httpUrl).getStats());
      return;
    }
    const queue = new Queue({ dbPath: resolve(stringArg(args, 'db', './queue.sqlite')!) });
    try {
      printJson(queue.stats(100_000));
    } finally {
      queue.close();
    }
    return;
  }

  help();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

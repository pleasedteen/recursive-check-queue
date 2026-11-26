# plugin-check-queue

TypeScript + Node.js + SQLite 的本地持久化队列，用于递归展开的插件兼容性检查。

- 调用方提交根检查、查询进度和最终结果。
- 多个独立工作进程领取任务、续租、提交子检查清单或结果。
- 大清单采用分段交接：Handler 可边生成边登记，整份确认前任何子项都不可领取。
- 一个工作进程同一时刻只有一个检查执行名额；父检查等待子项时进入 `waiting`，不占执行名额。
- 父检查恢复时，Worker 会按 ordinal 分页读回与清单对应的全部直接子结果；失败子项也是一条终态结果。
- 展开、确认和完成在 SQLite 写事务内落库，`SIGKILL`、进程崩溃或服务强停后可恢复；中断的交接可凭父项自己的有效租约接手重试。
- 租约到期后任务可被重新领取；旧 worker/fence/token 的追加、确认或状态提交都会被拒绝。
- 全局等待领取的 `ready` 项不超过 `readyLimit`。大清单全部持久化，但登记、领取补位、后台补位每次实际解码/读取的描述项都不超过 `readyLimit`。
- SQLite 中保存峰值和每次登记/清单读取量，可用 CLI/HTTP/API 核对。

运行环境：Node.js >= 22.5（在 Node.js 22.23 上验证，使用内置 `node:sqlite`，无运行时第三方依赖）。

## 安装与一条命令验证

```bash
npm install
npm run verify
```

`npm run verify` 会先编译，然后自动完成：

1. 短租约到期、重新领取、旧 token/fence 提交被拒绝。
2. `readyLimit=1` 时的跨树公平性：新的单步树根在提交后的第二次成功领取内一定被领到。
3. 120 个子项、`readyLimit=4` 的大清单（30 倍缓冲）：峰值为 4，所有调度读取和结果分页都不超过 4。
4. 启动一个真实 Worker 子进程，仅一个运行名额、`readyLimit=1`，跑完 4 层检查树，并验证失败子项结果和父项聚合。
5. 单 Worker、`readyLimit=1` 的宽清单夹具，覆盖确认前后的两处中断：
   - 确认前：Worker 在分段登记途中被 `SIGKILL`，交接停在 `open`，已登记前缀保留；替换 Worker 经父项有效租约接手（新的 fence），凭持久化的已登记偏移续传而非重放，旧租约无法追加或确认。
   - 确认后：处于 `waiting` 的父项还有尾段未派出时，服务与 Worker 同被 `SIGKILL`；重启后继续派发尾段、复用已完成子结果、父项按原序收齐（含业务失败项）。
6. 启动 HTTP 队列服务和 3 个 Worker 子进程，验证多进程领取。
7. 在运行中对服务和全部 Worker 发送 `SIGKILL`，重启服务和新 Worker，核对已确认展开/结果仍在、旧结果未被替换、每个逻辑子项只有一条结果，并且 8 棵 3 层树全部完成。

本机实测 `npm run verify` 总计约 10–13 秒，远低于 80 秒；`npm install` 时间另计。成功时最后一行类似：

```text
[verify 5.9s] PASS: all persistence, recovery, fairness, and limit checks passed in 5.9s
```

失败时临时数据库目录会保留，日志会打印该目录。

## 本地检查夹具

内置夹具位于 `src/fixtures.ts`，编译后由 CLI 的 `--fixtures` 加载：

| kind | 行为 |
| --- | --- |
| `fixture.ok` | 直接成功，返回 pid、尝试次数和输入回显。 |
| `fixture.fail` | 直接失败，可传 `{ "message": "..." }`。 |
| `fixture.slow` | 按 `ms`/`delayMs` 延迟，响应租约丢失的 AbortSignal。 |
| `fixture.tree` | 递归树。`depth > 0` 时交出 `breadth` 个同类子检查；`depth = 0` 时直接成功。可加 `includeFailure: true` 生成一个确定失败的 `required-bad-child`。恢复时返回完整有序子结果。 |
| `fixture.wide` | 扁平宽清单：`breadth` 个 `fixture.ok` 子项，`includeFailure: true` 时追加一个失败项；用于分段交接夹具。恢复时返回完整有序子结果。 |

`fixture.tree` 与 `fixture.wide` 都走分段交接（`openChecklist()` → 多次 `append()` → `commit()`），可用 `segment` 控制每段描述项数、`streamDelayMs` 在段间插入可中断延迟以便复现中断。

例如：

```json
{
  "depth": 3,
  "breadth": 3,
  "delayMs": 10,
  "includeFailure": true,
  "label": "demo"
}
```

`depth=3,breadth=3` 的不含失败树有 `1 + 3 + 9 + 27 = 40` 个逻辑检查。

## HTTP 启动方式

先编译：

```bash
npm run build
```

### 1. 启动队列服务

```bash
node --no-warnings dist/cli.js serve \
  --db ./data/queue.sqlite \
  --port 4318 \
  --ready-limit 1 \
  --lease-ms 10000
```

`--port 0` 可让系统分配端口；启动行输出实际端口：

```json
{"type":"listening","port":4318,"host":"127.0.0.1","db":"/.../queue.sqlite"}
```

服务也会周期性后台补位。可用 `--replenish-ms 250` 调整。

### 2. 启动多个工作进程

每个进程只创建一个串行执行循环，因此启动多个终端/进程即可得到多个运行名额：

```bash
node --no-warnings dist/cli.js worker \
  --http http://127.0.0.1:4318 \
  --fixtures \
  --worker-id worker-a \
  --poll-ms 20 \
  --heartbeat-ms 3000
```

再开一个终端：

```bash
node --no-warnings dist/cli.js worker \
  --http http://127.0.0.1:4318 \
  --fixtures \
  --worker-id worker-b
```

父检查调用 `expand()` 或返回 `{ expand: [...] }` 后，本次 handler 结束，进程的唯一名额立即释放，循环会继续领取别的任务。

### 3. 提交根检查

```bash
curl -sS -X POST http://127.0.0.1:4318/v1/checks \
  -H 'content-type: application/json' \
  -d '{
    "id": "root-demo",
    "kind": "fixture.tree",
    "input": {
      "depth": 3,
      "breadth": 3,
      "includeFailure": true
    }
  }'
```

如果就绪缓冲已满，根检查会持久化为 `staged`，等领取产生空位后按提交顺序进入 `ready`，不会丢失。

### 4. 查询进度和结果

```bash
curl -sS http://127.0.0.1:4318/v1/checks/root-demo
```

返回递归树的状态计数：

```json
{
  "rootId": "root-demo",
  "status": "completed",
  "total": 41,
  "completed": 40,
  "failed": 1,
  "terminal": 41,
  "percent": 100,
  "result": {
    "receivedChildCount": 4,
    "failedChildren": [{ "key": "required-bad-child" }]
  }
}
```

注意：上面的 `failed: 1` 是树中确定失败的子检查；夹具父检查会收到该失败结果并自行决定根仍为 `completed`。如果根 kind 本身是 `fixture.fail`，根状态会是 `failed`，响应中带序列化 error。

### 5. 查看统计

```bash
curl -sS http://127.0.0.1:4318/v1/stats
```

或：

```bash
node --no-warnings dist/cli.js stats --http http://127.0.0.1:4318
```

重点字段：

- `readyLimit`：就绪上限。
- `readyCurrent` / `readyPeak`：当前/峰值就绪项数。
- `listReads[]`：每次 `register`、`expand`、`claim`、`replenish`、`result_page` 的读取条数和操作前后 ready 数。
- `listReadMaxByOp`：各类操作的最大读取量（`register` 即每次登记分段实际解码的描述项数）。
- `claims` / `distinctClaimWorkers`：领取次数和不同 worker。
- `events[]`：提交、领取、续租、展开、完成、租约丢失等事件。

## 直接使用 SQLite，不启动 HTTP

多个 Worker 进程可以直接打开同一个 SQLite 文件；WAL 和 `BEGIN IMMEDIATE` 负责串行化领取：

```bash
# 终端 1
node --no-warnings dist/cli.js worker \
  --db ./data/direct.sqlite \
  --fixtures \
  --worker-id direct-a \
  --ready-limit 1

# 终端 2
node --no-warnings dist/cli.js submit \
  --db ./data/direct.sqlite \
  --ready-limit 1 \
  --kind fixture.tree \
  --input '{"depth":4,"breadth":2,"includeFailure":true}'

# 查询
node --no-warnings dist/cli.js inspect --db ./data/direct.sqlite --id <root-id>
node --no-warnings dist/cli.js stats --db ./data/direct.sqlite
```

如果省略 `--ready-limit`，后续进程会采用数据库中已经保存的上限；显式传入不同上限会报错，避免两个进程按不同缓冲不变量运行。

## TypeScript 编程接口

```ts
import { Queue } from './dist/queue.js';
import { DirectQueueBackend, Worker, type HandlerMap } from './dist/worker.js';

const queue = new Queue({
  dbPath: './data/programmatic.sqlite',
  readyLimit: 1,
  leaseMs: 10_000,
});

const handlers: HandlerMap = {
  'compat.root': async (ctx) => {
    // ctx.children === null：第一次执行，展开子检查并交还名额。
    if (ctx.children === null) {
      return {
        expand: [
          { key: 'os', kind: 'compat.os', input: { plugin: 'example' } },
          { key: 'runtime', kind: 'compat.runtime', input: { plugin: 'example' } },
        ],
      };
    }

    // 恢复执行：ctx.children 已按 ordinal 排好序；
    // 失败子项形如 { status: 'failed', error: { message, ... } }。
    return {
      childCount: ctx.children.length,
      failures: ctx.children.filter((child) => child.status === 'failed'),
      results: ctx.children,
    };
  },

  'compat.os': async ({ input, signal }) => {
    await doRealCheck(input, signal);
    return { osCompatible: true };
  },

  'compat.runtime': async () => {
    throw new Error('runtime range mismatch'); // 成为 failed 子结果
  },
};

const worker = new Worker(new DirectQueueBackend(queue), handlers, {
  workerId: 'worker-1',
  pollMs: 20,
  heartbeatMs: 3_000,
});

void worker.run();

queue.submitRoot({
  id: 'root-1',
  kind: 'compat.root',
  input: { pluginVersion: '1.4.0' },
});

const finalProgress = await queue.waitForTerminal('root-1', {
  timeoutMs: 30_000,
});
console.log(finalProgress.status, finalProgress.result);
```

也可以不显式返回 `{ expand }`，而是调用：

```ts
await ctx.expand([
  { key: 'a', kind: 'child.kind', input: {} },
]);
return; // handler 结束后父项等待，Worker 名额释放
```

大清单建议分段交，边生成边登记（整份确认前子项不可领取）：

```ts
'compat.wide': async (ctx) => {
  if (ctx.children) {
    return { received: ctx.children.length, results: ctx.children };
  }
  const writer = await ctx.openChecklist();
  for (let i = writer.registered; i < total; i += writer.readLimit) {
    await writer.append(makeDescriptors(i, writer.readLimit));
    // 中断后可凭 expansion.registered 从断点续传
  }
  const ack = await writer.commit(); // 之后子项才可领取
  return { registered: ack.totalChildren };
},
```

## HTTP 接口

| 方法与路径 | 说明 |
| --- | --- |
| `POST /v1/checks` | 提交根检查，body：`{id?, kind, input?}`。 |
| `GET /v1/checks/:id` | 获取递归进度；终态时包含 result/error。 |
| `POST /v1/claims` | body：`{workerId}`；无任务时返回 `204`。 |
| `POST /v1/checks/:id/heartbeat` | 续租，body 含 `workerId/leaseToken/fence`。 |
| `POST /v1/checks/:id/expand` | 一次性提交整份子清单并确认（数组 Handler 的正常入口）。 |
| `POST /v1/checks/:id/begin-expand` | 开启分段交接；parent 保持租约。 |
| `POST /v1/checks/:id/append-expand` | 追加一段描述项，body：`{children}`；单次不超过 `readyLimit`。 |
| `POST /v1/checks/:id/commit-expand` | 确认整份清单；此后子项才可领取，只提升一段就绪。 |
| `POST /v1/checks/:id/complete` | 提交 `completed`/`failed` 结果。 |
| `GET /v1/checks/:id/child-results?afterOrdinal=-1&pageSize=N` | 分页读取完整子结果。 |
| `POST /v1/replenish` | 手动触发后台补位。 |
| `GET /v1/stats` | 峰值、状态计数、每次登记/清单读取量、worker 事件。 |
| `GET /health` | 健康检查。 |

## 关键语义

### 就绪上限和大清单

两种交接方式共用同一条持久化路径：

- **一次性（数组）**：Handler 返回 `{ expand: [...] }` 或调用 `await ctx.expand([...])`。整份数组登记后立即确认，仍是数组 Handler 的正常入口。
- **分段（流式）**：Handler 调用 `await ctx.openChecklist()` 拿到写入器，边生成边 `append(descriptors)`，最后 `commit()`。写入器会自动把过大的数组切成不超过 `readyLimit` 的分段；`writer.registered` 是持久化的已登记偏移。

无论哪种方式，所有子描述符都写入 `checks` 表（状态 `staged`），但：

- 只有 `expansion_state = 'confirmed'` 的父项，其子项才可能被提升/领取；确认前子项一律不可领取；
- 登记（`register`）、确认（`expand`）、成功领取后的同事务补位、完成后的同事务补位、后台 `replenish` 都带 `LIMIT`，单次实际解码/读取行数不超过 `readyLimit`，并写入 `list_read_stats`；
- 结果恢复使用独立的 `child_results` 分页读取，每页也不超过 `readyLimit`；Worker 读完所有页后才调用恢复 handler。

### 分段交接的中断恢复

- 交接进行中父项仍是 `leased`，`expansion_state = 'open'`，子项仍 `staged`。
- 中断（进程崩溃、服务强停或租约过期）后，父项可被重新领取；新租约凭 `expansion_registered` 的持久偏移从断点续传，不会重放已提交分段。
- 旧租约的 `append-expand`/`commit-expand` 会因 `fence`/`leaseToken` 不匹配被拒绝（`LEASE_FENCED`/HTTP 409），无法追加或确认接手者的清单。
- 确认（`commit-expand`）后父项进入 `waiting` 并释放名额；即便服务和 Worker 同被 `SIGKILL`，重启后已确认父项的 `staged` 尾段仍会继续派发，已完成子结果被复用，父项按 ordinal 原序收齐（含业务失败项）。

### 公平性

当 ready 缓冲已满时，新根进入 `staged`。产生领取空位后，补位顺序优先：

1. 等待中的新根（按提交序号）；
2. 所有子项已结束、等待恢复名额的父项（按完成顺序）；
3. 各树的分段子清单。

在 `readyLimit=1` 且另一棵树已有一个 ready 项时：第一次成功领取取走该旧项，同一事务把新单步根提升为 ready；第二次成功领取必定领到新根。验证脚本显式覆盖这一点。

### 租约和 fencing

每次领取或重新领取都会：

- 生成新的随机 `leaseToken`；
- 单调递增 `fence` 和 `attempt`；
- 写入 `lease_expires_at`。

心跳、展开、完成都必须匹配当前 `workerId + leaseToken + fence` 且租约仍未到期。旧 worker 即使在租约到期后才发起提交，也会收到 `LEASE_FENCED`/HTTP 409，不会覆盖新执行结果。

### 持久化和 exactly-once 效果

- 数据库使用 WAL、`synchronous=FULL`、`busy_timeout=10000`。
- 领取、登记分段、确认、完成、统计更新都在 `BEGIN IMMEDIATE` 事务中。
- `child_results.child_id` 是主键；每个逻辑子项最多一行结果。
- 已终态检查不会再次被领取；重启后已有结果直接用于父项聚合。
- 验证脚本在 `SIGKILL` 前记录一条 `child_results.completed_at`，整树跑完后核对该时间未变。

## 目录

```text
src/types.ts      共享类型、错误、序列化
src/queue.ts      SQLite 持久队列、事务、调度、统计
src/worker.ts     单名额 Worker 运行时、心跳、恢复分页
src/client.ts     HTTP Worker/调用方客户端
src/server.ts     HTTP JSON API
src/fixtures.ts   本地递归检查夹具
src/cli.ts        serve/worker/submit/inspect/stats
src/verify.ts     一条命令的完整验证
```

# plugin-check-queue

TypeScript + Node.js + SQLite 的本地持久化队列，用于递归展开的插件兼容性检查。

- 调用方提交根检查、查询进度和最终结果。
- 多个独立工作进程领取任务、续租、提交子检查清单或结果。
- 一个工作进程同一时刻只有一个检查执行名额；父检查等待子项时进入 `waiting`，不占执行名额。
- 父检查恢复时，Worker 会按 ordinal 分页读回与清单对应的全部直接子结果；失败子项也是一条终态结果。
- 展开和完成在同一个 SQLite 写事务内落库，`SIGKILL`、进程崩溃或服务强停后可恢复。
- 租约到期后任务可被重新领取；旧 worker/fence/token 的状态提交会被拒绝。
- 全局等待领取的 `ready` 项不超过 `readyLimit`。大清单全部持久化，但展开登记、领取补位、后台补位每次最多读取 `readyLimit` 条清单记录。
- SQLite 中保存峰值和每次清单读取量，可用 CLI/HTTP/API 核对。

运行环境：Node.js >= 22.5（在 Node.js 22.23 上验证，使用内置 `node:sqlite`，无运行时第三方依赖）。

## 安装与一条命令验证

```bash
npm install
npm run verify
```

`npm run verify` 会先编译，然后自动完成：

1. 短租约到期、重新领取、旧 token/fence 提交被拒绝。
2. `readyLimit=1` 时的跨树公平性：新的单步树根在提交后的第二次成功领取内一定被领到。
3. 120 个子项、`readyLimit=4` 的大清单（30 倍缓冲）：峰值为 4，所有调度读取和结果分页都不超过 4；统计按实际读取行数报（小清单实际读 2 就记 2，而不是 LIMIT 4）。
4. 交接重试（handoff retry）：同一 `workerId + leaseToken + fence` 重复提交 `expand`（直连和真实 HTTP 各验一遍，含模拟回包丢失后的客户端自动重试）返回同一份 ack，子项和交接回执都只登记一次；伪造 token/worker/fence 的重试返回 `LEASE_FENCED`，不能写入或派发。
5. 启动一个真实 Worker 子进程，仅一个运行名额、`readyLimit=1`，跑完 4 层检查树，并验证失败子项结果和父项聚合。
6. 启动 HTTP 队列服务和 3 个 Worker 子进程，验证多进程领取。
7. 运行中对服务和全部 Worker 发送 `SIGKILL`，重启服务和新 Worker：核对所有 kill 前已终态子结果的 `completed_at` 不变、事件水位之后没有任何针对这些子项的再次领取、每个逻辑子项只有一条结果，并且 8 棵 3 层树全部完成。
8. 200 个子项、`readyLimit=4`（50 倍缓冲）的宽清单：等到完整交接确认（事件含全部 200 项、进度 total=201、交接回执落盘）后立刻同时 `SIGKILL` 服务和唯一原 Worker；重启服务与两个新 Worker 后整树跑完，峰值和 expand/claim/replenish 实际读取均不超过 4，父项按 ordinal 原顺序收到全部 200 个结果（含位于末尾的确定失败子项），已完成子项不被再次领取。

本机实测 `npm run verify` 总计约 6–8 秒，远低于 80 秒；`npm install` 时间另计。成功时最后一行类似：

```text
[verify 5.4s] PASS: actual-read limits, handoff retry/idempotency, fencing, no re-claim of completed children, ordered results incl. failures, and SIGKILL handoff recovery all verified in 5.4s
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
- `listReads[]`：每次 `expand`、`claim`、`replenish`、`result_page` 的读取条数和操作前后 ready 数。
- `listReadMaxByOp`：各类操作的最大读取量。
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

## HTTP 接口

| 方法与路径 | 说明 |
| --- | --- |
| `POST /v1/checks` | 提交根检查，body：`{id?, kind, input?}`。 |
| `GET /v1/checks/:id` | 获取递归进度；终态时包含 result/error。 |
| `POST /v1/claims` | body：`{workerId}`；无任务时返回 `204`。 |
| `POST /v1/checks/:id/heartbeat` | 续租，body 含 `workerId/leaseToken/fence`。 |
| `POST /v1/checks/:id/expand` | 提交子清单并把父项置为 `waiting`；同一租约一代重复提交幂等重放。 |
| `POST /v1/checks/:id/complete` | 提交 `completed`/`failed` 结果。 |
| `GET /v1/checks/:id/child-results?afterOrdinal=-1&pageSize=N` | 分页读取完整子结果。 |
| `POST /v1/replenish` | 手动触发后台补位。 |
| `GET /v1/stats` | 峰值、状态计数、每次清单读取量、worker 事件。 |
| `GET /health` | 健康检查。 |

## 关键语义

### 就绪上限和大清单

展开时，所有子描述符都会写入 `checks` 表（状态为 `staged`），但只按 `parent_id, ordinal` 读取并提升最多当前空位数量，且不超过 `readyLimit`。后续由：

- 成功领取后的同事务补位；
- 完成后的同事务补位；
- 服务/Worker 的后台 `replenish`；

继续分段读取。相关 SQL 都带 `LIMIT`，实际读取行数写入 `list_read_stats`。

结果恢复使用独立的 `child_results` 分页读取，每页也不超过 `readyLimit`；Worker 会读完所有页后再调用恢复 handler。因此调度扫描有界，而父项仍能拿到完整结果。

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
- 领取、展开、完成、统计更新都在 `BEGIN IMMEDIATE` 事务中。
- 展开在一个事务内一次解码并写入全部子描述符、把父项置为 `waiting`、写入交接回执并提升首个有界就绪段；提交后父项的执行名额才在 Worker handler 返回时释放，子项才已被派发。
- `operation_receipts(check_id, op, fence)` 保存每次 `expand` 交接的原始响应。回包丢失后用同一份 `workerId + leaseToken + fence` 重试会只读重放原响应，不重复登记子项；伪造或更新一代的租约凭据返回 `LEASE_FENCED`，不能写入或派发。HTTP 客户端对 `expand` 的网络错误/5xx 会用同一租约负载自动重试。完成提交仍由条件更新 fencing，`child_results` 主键保证结果行不重复，Worker 把重试导致的 409 当作租约已交接的良性结果。
- `child_results.child_id` 是主键；每个逻辑子项最多一行结果。
- 已终态检查不会再次被领取；重启后已有结果直接用于父项聚合。
- 验证脚本在 `SIGKILL` 前记录每条 `child_results.completed_at` 和事件 id 水位，整树跑完后核对时间未变且水位之后没有针对这些子项的新 `claimed` 事件。

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

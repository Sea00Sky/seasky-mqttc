# 并发约定

两种用法，规则完全不同，别混着理解：

| | `Client`（核心） | `AsyncClient`（外壳） |
|---|---|---|
| 线程 | **单线程**：谁调 `pump()` 就是谁 | 自带 IO 线程 + 投递线程 |
| 谁推进状态机 | 调用方，按自己的节奏调 `pump(now_ms)` | IO 线程每个 tick 调一次 |
| 时间来源 | 调用方传进来 | 内部 `monotonic_ms()` |
| 回调在哪个线程 | 调 `pump()` 的那个线程 | 投递线程（与 IO 线程分开） |
| 背压 | 回调返回 `false` 就丢 | 回调返回 `false` 就丢；入站队列满了也丢 |

**直接多线程用同一个 `Client` 是不支持的**：它的成员没有任何同步，也不打算有。要并发就用
`AsyncClient`，或者自己在外面串行化。

## AsyncClient 的线程分工

| 线程 | 干什么 | 绝不干什么 |
|---|---|---|
| 调用方线程 | 调 `start()` / `stop()` / `publish()` / `subscribe()` / `unsubscribe()` / `set_handler()` / `snapshot()` 等 | 不碰 `Client`、不碰 `Transport` |
| IO 线程 | 取空命令队列 → 执行命令 → `pump()` → 更新快照 → 等 tick | 不调用户回调 |
| 投递线程 | 从入站队列取消息 → 调用户回调 | 不碰 `Client`、不碰 `Transport` |

只有 IO 线程碰 `Client` 与 `Transport`。这条不是"锁加得多"，而是并发正确性能讲清楚的全部依据。

## 从哪些线程能调什么

| 方法 | 调用方线程 | 投递线程（回调里） | 说明 |
|---|---|---|---|
| `start()` / `stop()` | 可 | 不可 | 回调里 `stop()` 是投递线程 join 自己 → 死锁（Debug 下会断言） |
| `publish()` / `subscribe()` / `unsubscribe()` | 可 | 可 | 只入队，不阻塞；回调里再发消息是正常用法 |
| `set_handler()` | 可 | 可 | 换掉之后旧回调不再被调用（在飞的那一条仍按取出来的那份调） |
| `snapshot()` / `state()` / `stats()` / `queued_inbound()` | 可 | 可 | 返回拷贝 |
| `wait_for_state()` | 可 | 不可 | 会阻塞等，回调里等状态没有意义，还可能等不到 |

## 返回值到底代表什么

`publish()` / `subscribe()` / `unsubscribe()` **不阻塞**，返回值说的是"入队这一步的结果"：

| 返回 | 含义 |
|---|---|
| `Error::None` | 已入队，**不代表已经发出去了**。真正发出前的连接状态与在途上限由 IO 线程再判一次 |
| `Closed` | 还没 `start()`、已经 `stop()`，或当前快照不是 `Connected` |
| `Busy` | 命令队列满（`command_queue_capacity`）；调用方自己决定等、丢还是重试 |
| `ProtocolError` | 主题名 / 过滤器非法。这类在调用线程就判掉，不占队列（纯函数校验，很便宜） |

命令已经入队但执行时发现"没连上 / 在途已满"的，会被计进 `snapshot().rejected_commands`，
并按 `warn` 级别记一条 `command_rejected`。所以"入队成功"与"真的发出去了"要分开看：
前者看返回值，后者看 `snapshot().stats.published` 与 `inflight`。

## 状态是快照，会滞后

`state()` / `stats()` / `snapshot()` 读的是 IO 线程每 tick 在锁下写的一份拷贝：

- 好处：不用把内部成员暴露给别的线程，读的时候不会跟正在改的成员打架。
- 代价：最多滞后一个 `tick_ms`。`publish()` 刚返回、`state()` 还没变成 `Connected` 是正常的。
- 要"等到某个状态"就用 `wait_for_state(state, timeout_ms)`，别自己 `sleep` 猜。

## 回调的规矩

1. **不持锁调用**：库把 handler 的 `shared_ptr` 拷出来、解锁之后才调。所以回调里可以再
   `publish()`，可以 `set_handler()`，也可以花时间处理。
2. **别在回调里 `stop()`**：那是投递线程 join 自己，会死锁。要停就交给别的线程。
3. **返回 `false` = 处理不过来**：该条丢弃（`stats.dropped` 与 `snapshot().dropped_inbound`
   都会涨），QoS1 也仍然回 PUBACK。想不丢就自己在回调里排队，或者把队列上限调大。
4. **回调慢不会拖住 IO 线程**：keepalive、重连、读数据都在 IO 线程上照常跑
   （`tests/test_async.cpp` 里有一条用例专门锁这个：回调占着 3 秒，这段时间的 ticks 至少涨 20）。
   但回调慢会让投递积压，积压到队列上限就开始丢。

## 停机顺序

```
stop():  置停止位 → 唤醒 IO 与投递 → join(IO) → 宣布"队列不会再增长" → join(投递)
```

- IO 线程先退出（它在自己线程里发 DISCONNECT 并关传输层）。
- 投递线程随后**把队列排空**才退出，所以已经收到的消息不会因为停机被丢。
- `stop()` 幂等；同时从两个线程调 `stop()` 不受支持（只有一个能走到 join）。
- 析构函数会调 `stop()`，所以 `AsyncClient` 对象活着的时候线程就活着。

## 选 tick

不变量：`tick_ms ≤ min(keepalive_ms / 4, publish_retry_ms / 4)`。默认 `tick_ms = 10`（毫秒），
默认 `keepalive = 60` 秒，所以默认值有足够裕量。tick 太粗，定时器精度就会吃掉 keepalive 的裕量；
tick 太细纯属浪费 CPU（每次 tick 会做一次读与一次快照）。

## 传输层的前提

`read()` **必须有界**：自带的两份实现都基于 `poll`/`select` 超时返回 `WouldBlock`。
如果换成会无限阻塞在 `read()` 上的传输层，`stop()` 就一直等它——`stop()` 不会从别的线程去
`close()` 传输层，因为那会破坏"只有 IO 线程碰 transport"这条规则。

## 队列上界与丢包

| 队列 | 上限 | 满了怎么办 |
|---|---|---|
| 命令队列（应用线程 → IO 线程） | `command_queue_capacity`（默认 1024） | `publish()` 直接返回 `Busy`，不阻塞、不静默丢 |
| 入站队列（IO 线程 → 投递线程） | `inbound_queue_capacity`（默认 256 条）与 `inbound_max_bytes`（默认 4 MiB）同时限 | 丢掉新来的那条并计数（`stats.dropped`、`dropped_inbound`） |

`tests/test_async.cpp` 里有用例把上限压到 4 条、喂 40 条，断言
"交给回调的 + 丢掉的 = 喂进去的"——账必须对得上。

## 怎么验证这块

- `tests/test_async.cpp`：多线程发布、慢回调不阻塞 IO 线程、队列满丢弃与账目、回调里再
  publish、`stop()` 与 `publish()` 竞争、运行中换回调、停机排空。连跑多次结果一致。
- **TSan 已在 Linux 上实跑且干净**（WSL2 Ubuntu 24.04.1，GCC 13.3.0，
  `-DMQTTC_SANITIZE=thread`）：`129 cases, 0 failed`、`ctest` 2/2（含真 socket 的 e2e）。
  前提是 `sysctl vm.mmap_rnd_bits=28`：Ubuntu 24.04 默认 32，TSan 的影子映射装不下，
  会在跑任何用例之前直接退出（`FATAL: ThreadSanitizer: unexpected memory mapping`，
  exit 66）——那是环境问题，不是竞争报告；CI 的 TSan job 里有同样的一步。
  Windows/MinGW 没有 TSan，本地不跑。
- TSan 干净只说明**这批用例覆盖到的路径上**没查出竞争，不等于"任何用法都无竞争"。
  结构约定（锁不嵌套、只有 IO 线程碰 `Client`/`Transport`、handler 在锁外调用）仍然是
  第一道约束，用例与 TSan 是第二道。

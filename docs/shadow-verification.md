# 服务端影子校验方案

> 状态：设计方案，尚未实现。本文描述目标实现，不代表当前代码行为。现行线程划分见 [连接路由与线程模型](threading-model.md)，现行报文契约见 [数据面协议](protocol.md)。

影子校验指服务端在帧同步广播之外，额外加载客户端导出的原生模拟库（Windows `.dll` / Linux `.so`），用与客户端完全相同的逻辑推进一份权威模拟，并把权威状态哈希与客户端上报的状态哈希逐校验点比对，用于检测状态分歧与客户端作弊。

## 1. 范围与不变量

- 帧广播路径完全不变：`ServerFrame` 的生成、玩家顺序、no-op 补齐、历史回放、心跳与重连逻辑均不受影响。
- 影子模拟是**旁路消费者**，消费的是已经广播出去的那一份 `ServerFrame` 字节，因此服务端与客户端喂给模拟的输入在设计上就是同一份数据。
- 影子模拟**永不反压帧推进**。影子落后、原生报错、原生库缺失，最坏后果都只是该房间失去校验能力，对局照常进行。
- v1 只校验「客户端是否忠实执行了服务端下发的帧」，不校验输入本身是否合法。
- 首版为进程内加载。原生崩溃会带走整个 JVM，这是已知取舍，见[第 11 节](#11-已知取舍)。

## 2. 模块划分

新增包 `com.rainnov.lockstep.shadow`：

| 类型 | 职责 |
| --- | --- |
| `ShadowLibrary` | 原生库加载、ABI 版本与 build-id 校验、下行 `MethodHandle` 持有者、节点级熔断 |
| `ShadowNative` | FFM 下行句柄与结构体内存布局常量 |
| `ShadowWorkerGroup` | 影子线程组（平台线程），房间到线程的固定映射 |
| `ShadowInstance` | 单房间影子实例：native handle + 私有 `Arena` + 暂存 `MemorySegment` + 背压计数 |
| `ShadowVerifier` | 对房间暴露的边界接口，房间侧只依赖它 |
| `ShadowSink` | 权威哈希回投接口，由房间侧实现 |
| `ShadowState` | `DISABLED` / `PENDING` / `RUNNING` / `DEGRADED` / `MISMATCHED` |
| `ShadowDegradeReason` | `BACKLOG` / `NATIVE_ERROR` / `STEP_BUDGET` / `CLIENT_AHEAD` / `CREATE_FAILED` |

`GameRoom` 只依赖 `ShadowVerifier` 与 `ShadowSink`，与现有 `RoomCommandGateway` 的解耦风格一致，纯 Java 假实现即可完成房间侧全部单元测试。

## 3. 原生库 ABI 契约

```c
uint32_t     ls_shadow_abi_version(void);            // 必须等于服务端常量
const char*  ls_shadow_build_id(void);               // 与分配请求携带的规则版本比对
int32_t      ls_shadow_create(const ls_create_params*, void** out_handle);
int32_t      ls_shadow_step(void* h, const uint8_t* blob, size_t len, ls_step_result* out);
int32_t      ls_shadow_destroy(void* h);
const char*  ls_shadow_last_error(void* h);
uint64_t     ls_shadow_footprint(void* h);
```

硬性要求，违反其中任何一条都会使本文的线程与内存推理失效：

- 纯 C ABI，C++ 异常不得跨越边界，错误一律用返回码。
- **不得回调进 JVM**，**不得自建线程**。
- 不读墙钟、不读 locale、不读环境变量；随机数只由 `create` 传入的 seed 驱动。
- 浮点必须是定点数或严格受控，编译选项与客户端出包一致。
- 所有传入指针仅在单次调用期间有效。

`ls_shadow_step` 接收的是**序列化后的 `ServerFrame` protobuf 字节**，而非自定义结构体。这样原生侧可直接复用客户端自己的解帧代码路径，不依赖两边手写内存布局对齐。

`ls_step_result` 布局：

```c
typedef struct {
    uint64_t state_hash;   // offset 0
    uint32_t frame_id;     // offset 8
    int32_t  status;       // offset 12
} ls_step_result;          // 按 8 字节对齐，服务端按 32 字节分配留余量
```

## 4. 加载与启动顺序

- 库路径 `lockstep.shadow.library-path` 必须是**绝对路径**。禁止依赖 `PATH` / `LD_LIBRARY_PATH` 搜索，避免供应链劫持。
- 启动时记录库文件 SHA-256 与 `build_id` 到日志；`abi_version` 与服务端常量不符直接拒绝加载。
- `build_id` 与 `POST /rooms` 携带的规则版本不一致时，分配请求返回 422，不允许用错版本的影子去校验。
- 库加载放在 `ShadowLibrary` 的 bean 初始化阶段（构造即失败），早于所有 `SmartLifecycle`。
- 线程组创建与预热放在 `SmartLifecycle` **phase 50**。预热在每条影子线程上各跑一次「create → step 空帧 → destroy」，作用是提前触发缺页、编译下行调用桩，并在启动期就暴露库缺陷。

启动升序 数据面(0) → 影子(50) → 房间池(100)，保证建池前影子子系统已可用；停机降序 房间排空(100) → 影子关闭(50) → 关监听(0)，保证仍有房间运行时影子不会先被拆除。

JVM 参数需补 `--enable-native-access=ALL-UNNAMED`（JDK 25 下不加会持续告警），生产环境另加 `--illegal-native-access=deny`。需同步写入 `build.gradle` 的 `bootRun` / `JavaExec` 参数与部署脚本。

## 5. 线程模型

在现有四组线程之外新增第五组，**不复用房间线程**：

| 线程组 | 数量 | 职责 | 独占的可变状态 |
| --- | --- | --- | --- |
| `lockstep-shadow` | `lockstep.shadow.worker-threads`，默认 `min(4, CPU/2)` | 影子实例 create / step / destroy、帧序列化、取回状态哈希 | `ShadowInstance` 的 native handle、`Arena`、暂存 `MemorySegment` |

实现采用 `DefaultEventExecutorGroup(n, new DefaultThreadFactory("lockstep-shadow", true))`，与 `NettyRoomEventLoopProvider` 完全同构。

### 5.1 约束与理由

**必须是平台线程，不能是虚拟线程。** 原生调用可达毫秒级且大概率使用线程局部存储，虚拟线程会 pin 载体线程；`Arena.ofConfined()` 的所有权绑定到具体线程，虚拟线程调度会破坏「同一逻辑线程」这个前提。

**房间与影子线程终生固定绑定。** 房间在 `activate` 时从 `ShadowWorkerGroup.next()` 轮询取一条 `EventExecutor` 并保存。native 状态、confined `Arena`、可能的 TLS 都要求 create / step / destroy 在同一条 OS 线程上执行。

**房间线程上的成本接近零。** `runTick()` 在 `broadcast(envelope)` **之后**插入一次投递，传递的是不可变 protobuf 对象引用，序列化在影子线程上做：

```java
broadcast(envelope);
shadow.submitFrame(currentFrame, envelope.getServerFrame());   // 仅一次 execute
publishSnapshot();
```

房间线程增加的开销是一次小对象分配加一次入队，微秒级，不污染 `lastTickLagNanos`。

**顺序由单线程执行器天然保证。** `create` 任务先入队，`step` 排在其后，`destroy` 排在所有 `step` 之后。「实例尚未就绪时第 1 帧已到达」不需要额外状态机。

**背压不能靠丢帧。** 确定性模拟要求帧连续，丢一帧后续全部作废。使用 per-room `AtomicInteger backlog`：投递时自增，step 执行时自减。超过 `max-backlog-frames` 时该房间转 `DEGRADED`，立即释放实例，之后不再投递，只记指标。

**校验判定回到房间线程。** 影子线程只算哈希不做决策，通过 `ShadowSink` 回投到房间的 `EventExecutor`。终止房间、广播 `MatchEvent`、修改 `matchPhase` 的所有者本来就是房间线程，把值搬进去比把决策搬出来简单，且不需要任何锁。

**不使用 `Linker.Option.critical`。** 默认下行调用会将线程切换到 native 状态，不阻塞安全点；`critical` 让线程留在 Java 状态，长原生调用会直接拖住 GC 停顿。性能收益不值这个风险。

### 5.2 线程预算

单房间 step 的平均耗时必须小于 `帧周期 / 单线程房间数`。默认 20 Hz、16 房间、4 条影子线程 → 每线程 4 个房间 → 预算 12.5 ms/帧。配置 `step-budget` 默认 8 ms，`ShadowInstance` 统计 p99，越界即转 `DEGRADED`。

同一影子线程上的房间是协作式共享，某房间的慢 step 会拖慢同线程其他房间的校验进度。但由于影子不反压帧推进，最坏后果只是这些房间转 `DEGRADED`，对局本身不受影响。这个隔离性质优于房间线程组，也是把影子单独拉出一组线程的核心收益。

## 6. 内存模型

分四层。

### 6.1 加载层（进程生命周期）

一个 `Arena.ofShared()` 承载 `SymbolLookup.libraryLookup(path, arena)`，随 `ShadowLibrary` bean 销毁而关闭。下行 `MethodHandle` 不可变且线程安全，声明为 `static final` 以便 JIT 内联：

```java
private static final Linker LINKER = Linker.nativeLinker();

static final MethodHandle STEP = LINKER.downcallHandle(
    LOOKUP.findOrThrow("ls_shadow_step"),
    FunctionDescriptor.of(JAVA_INT,      // ls_status
        ADDRESS,                         // handle
        ADDRESS, JAVA_LONG,              // frame blob + len
        ADDRESS)                         // out: ls_step_result*
);
```

### 6.2 实例层（房间生命周期，堆外）

每个 `ShadowInstance` 持有一个 `Arena.ofConfined()`，**在影子线程上创建、在影子线程上关闭**。Arena 内一次性分配、终生复用，稳态零分配：

| 段 | 大小 | 用途 |
| --- | --- | --- |
| `frameBlob` | `max-frame-bytes`，默认 16 KiB | 序列化后的 `ServerFrame` 字节 |
| `stepResult` | 32 B | `ls_step_result` |
| `createParams` | ~1 KiB | roomId / matchId / seed / 玩家顺序表 |
| `errorBuf` | 256 B | `ls_shadow_last_error` 拷出点 |

16 KiB 的依据：8 玩家 × (1 KiB payload + tag/seq/playerId) + 帧头，上限约 8.5 KiB，留一倍余量。16 房间满载时 Java 侧堆外总量 ≤ 256 KiB。

真正的量在原生内部状态，用 `native-memory-budget-per-room`（默认 4 MiB）× 房间数做容量声明，并通过 `ls_shadow_footprint()` 上报实测值。建议开启 NMT 观察 `Other` 区。

### 6.3 序列化零中间拷贝

不经过 `byte[]`，直接写入常驻堆外段：

```java
ByteBuffer view = frameBlob.asByteBuffer();
CodedOutputStream out = CodedOutputStream.newInstance(view);
frame.writeTo(out);
out.flush();
long len = view.position();
```

### 6.4 跨线程发布与堆内结构

- 房间线程传给影子线程的只有 `ServerFrame`（protobuf 生成类，构建后不可变）与基本类型，配合执行器提交的 happens-before，不需要 `volatile` 也不需要防御性拷贝。
- **堆外段绝不跨线程共享。** confined `Arena` 在越界访问时抛 `WrongThreadException`，这是需要的保护而不是要绕开的限制。
- **不把堆内存暴露给原生。** 不使用 `MemorySegment.ofArray`，不启用 `allowHeapAccess`，避免与 GC 移动对象交互。
- 房间侧校验点哈希用两个定长数组构成环，容量 = `historyFrames / checkpoint-interval`，配合已有的 1000 帧回放窗口。用原生数组而非 `Map<Long, Long>`，避免装箱与每帧垃圾，约 1.6 KiB/房间。
- GC 特征：稳态每帧每房间仅产生一个投递任务对象（约 32 B）。20 Hz × 16 房间 = 320 obj/s，全部朝生夕灭。

## 7. 校验结果回传与对账

### 7.1 回传只有一跳

FFM 调用本身是同步的，结果写入影子线程私有的 `stepResult` 段并在同一线程读回为 Java `long`。堆外到堆内的转换全程在影子线程内完成。

```java
// ShadowInstance，影子线程上执行
private void stepOnShadowThread(long frameId, ServerFrame frame) {
    backlog.decrementAndGet();
    if (aborted || handle == null) {
        return;
    }
    long len = serializeInto(frameBlob, frame);
    int status = (int) ShadowNative.STEP.invokeExact(handle, frameBlob, len, stepResult);
    if (status != LS_OK) {
        degrade(ShadowDegradeReason.NATIVE_ERROR, readLastError());
        return;
    }
    if (frameId % checkpointInterval == 0) {
        long hash = stepResult.get(ValueLayout.JAVA_LONG, HASH_OFFSET);
        sink.acceptAuthoritativeHash(frameId, hash);
    }
}
```

```java
// ShadowSink 的房间实现，唯一的跨线程边界
@Override
public void acceptAuthoritativeHash(long frameId, long hash) {
    EventExecutor loop = room.eventExecutor();
    if (loop.isShuttingDown()) {
        return;
    }
    loop.execute(() -> room.onAuthoritativeHashOnLoop(frameId, hash));
}
```

不用 `volatile` 字段、不用 `CompletableFuture`、不用锁：`execute` 提供的可见性对一个 `long` 已经足够，而判定动作的所有者本来就是房间线程。

### 7.2 房间线程上的双向对账

两条数据流在房间线程汇合：影子权威哈希（经上述回投）与客户端上报哈希（经已有的 worker → 协调器 → 房间链路）。到达先后不确定，因此需要双向 pending 结构，全部为房间线程独占的定长数组：

```java
// GameRoom 字段
private final long[] checkpointFrames;      // 容量 historyFrames / checkpointInterval
private final long[] checkpointHashes;
private long shadowFrame;                   // 影子已推进到的校验点

// PlayerSlot 字段：待对账小环
private final long[] pendingFrames = new long[4];
private final long[] pendingHashes = new long[4];
```

```java
void onAuthoritativeHashOnLoop(long frameId, long hash) {
    if (state != RoomState.ACTIVE || matchPhase != MatchPhase.RUNNING) {
        return;                                  // 房间已终止，丢弃在途结果
    }
    int slot = (int) ((frameId / checkpointInterval) % checkpointHashes.length);
    checkpointFrames[slot] = frameId;
    checkpointHashes[slot] = hash;
    shadowFrame = frameId;
    for (PlayerSlot player : players.values()) {
        int hit = player.takePending(frameId);   // 兑现客户端先到的上报
        if (hit >= 0) {
            evaluate(player, frameId, player.pendingHashes[hit], hash);
        }
    }
}

void acceptStateHashOnLoop(String playerId, String sessionId, long frameId, long hash) {
    PlayerSlot player = requireCurrentSession(playerId, sessionId);
    touch(player);
    Long authoritative = lookupCheckpoint(frameId);
    if (authoritative != null) {
        evaluate(player, frameId, hash, authoritative);
    } else if (frameId > shadowFrame && !player.offerPending(frameId, hash)) {
        degradeShadow(ShadowDegradeReason.CLIENT_AHEAD);
    }
    // frameId <= shadowFrame 但查不到：已被环淘汰，计 missing，不判分歧
}
```

待对账环深度与背压阈值必须绑定：`max-backlog-frames = pending 环深度 × checkpoint-interval`（默认 4 × 10 = 40）。否则影子落后数个校验点时会持续吞掉客户端上报却不告警。

## 8. 协议扩展（v1 兼容）

按现有兼容规则，新增 `oneof` 选项、可选标量字段与枚举值均不需要升 v2。

低延迟主通道是把哈希**捎带在 `ClientInput` 上**，零额外报文：

```proto
message ClientInput {
  uint32 target_frame = 1;
  uint32 sequence = 2;
  bytes  payload = 3;
  // 新增：发送时刻已完整应用的帧及其状态哈希，零表示不上报
  uint32 applied_frame = 4;
  fixed64 applied_state_hash = 5;
}
```

玩家该周期没有输入时用独立消息兜底：

```proto
// Envelope.payload 新增
ClientStateHash client_state_hash = 18;

message ClientStateHash {
  uint32 frame_id = 1;
  fixed64 state_hash = 2;
}

// EventType 新增
EVENT_TYPE_DESYNC_DETECTED = 7;
// ProtocolErrorCode 新增
PROTOCOL_ERROR_CODE_INVALID_CHECKPOINT_FRAME = 13;
```

`ServerHello` 增加 `checkpoint_interval_frames = 15`，下发权威校验节奏，客户端不得写死。`ClientStateHash` 走与 `ClientInput` 相同的路由链路，同样刷新活动时间。

改动 `.proto` 后需重新生成兼容性测试向量，方法见[数据面协议](protocol.md#兼容性测试向量)。

## 9. 终止路径与延迟预算

### 9.1 终止是纯内存操作

判定成立后调用现有的 `terminateOnLoop(TerminationMode.GRACEFUL, TerminationReason.DESYNC, false)`，全程在房间线程同步完成，微秒级：取消 `tickTask` → 广播 `MATCH_TERMINATING` / `MATCH_ENDED` → 逐个 `safeClose(session, 4010, "DESYNC")` → 清 `pendingInputs` / `history` → 状态转 `TERMINATED` → `terminalListener` 回调 → 协调器补位。

三处必须做对，否则「及时」会变成「卡住」：

- **原生 destroy 不进终止路径。** `terminateOnLoop` 只调一次 `shadow.release()`：置 `aborted = true`，然后向影子线程 `execute(destroy + arena.close())`。房间不等待任何原生返回。`ls_shadow_destroy` 可能耗时数十毫秒，放在房间线程上会抖动同线程其他房间的帧推进。
- **`aborted` 是整个方案里唯一需要 `volatile` 的字段。** 房间线程写，影子线程在每个 step 任务开头读。没有它，release 之前已入队的数十个 step 会全部白跑；有了它，剩余任务立即返回。这是「影子线程独占状态」的唯一例外，实现时需单独注释。
- **在途哈希要能丢。** `onAuthoritativeHashOnLoop` 开头的状态判断即为此：release 与最后一个 step 之间存在竞态，房间已 `TERMINATED` 后仍可能收到一次回投。

`SessionCloseCodes` 新增 `DESYNC_DETECTED = 4010`，`TerminationReason` 新增 `DESYNC`。后者不在 `RoomMetrics.recordTermination` 的正常原因白名单内，会自动计入 `abnormalTerminations`，无需改动指标代码。证据（帧号、玩家、期望与实际哈希、影子 build-id）挂在 `RoomSnapshot` 新增的 `desyncReport` 字段上，经现有 `terminalListener` 送达控制面。

### 9.2 检测延迟

| 环节 | 典型 | 最坏 |
| --- | --- | --- |
| 校验点间隔 | 250 ms（interval = 10 的一半） | 500 ms |
| 客户端应用帧并上报到达服务端 | 60~120 ms | 网络抖动上限 |
| 影子 step 排队与执行 | < 50 ms（1 帧内） | 背压阈值 40 帧 = 2 s |
| 房间线程排队、判定、终止 | < 5 ms | < 50 ms |
| **端到端** | **≈ 350 ms** | **≈ 1 s（子系统健康时）** |

`checkpoint-interval` 是最大的延迟贡献项。由于哈希捎带在 `ClientInput` 上不产生额外报文，该值可下调至 1 实现逐帧校验（检测延迟 ≈ 120 ms），上行开销几乎不变。

## 10. 判定策略

分歧不等于作弊。一票否决整局是错的，会被一个版本错误的原生库屠掉整个节点。因此以影子为权威，但不盲信影子：

| 观察 | 结论 | 动作 |
| --- | --- | --- |
| 单个玩家连续 K 个校验点不一致（K 默认 2） | 该客户端状态偏离 | 关闭该玩家会话（4010），标记 `COMPLETED`，后续帧填 no-op，对局继续，上报证据 |
| 半数以上玩家在同一帧同时不一致 | 更可能是服务端影子版本或构建错误 | 房间影子转 `DEGRADED` 并释放实例，**不终止对局**，告警 |
| 全部玩家不一致且跨多个房间出现 | 节点级原生库错误 | `ShadowLibrary` 全局熔断，节点所有房间关闭影子，告警 |
| 客户端漏报 / 影子降级 | 无权威值可比 | 只计 `missing` / `degraded` 指标，永不触发终止 |
| 剩余玩家不足以继续 | 对局失去意义 | `terminate(GRACEFUL, DESYNC)` |

默认策略是踢出玩家而非终止整局：对「一个作弊者加三个正常玩家」的场景更合理，也让误判代价可控。`mismatch-policy` 可配为 `log-only`（灰度期）/ `kick-player`（默认）/ `terminate`。

### 能力上限

哈希由客户端自行计算并上报。被完整逆向的客户端可以一边本地作弊、一边上报「正确」哈希，此时影子校验检测不到任何异常，及时终止也无从触发。

因此影子校验的核心价值不在「终止作弊者」，而在**服务端持有权威状态**：结算以影子的模拟结果为准，而不是相信客户端上报的战斗结果，作弊者即便伪造哈希收益也归零。终止是止损补充，不是主防线。

若目标包含防住能伪造哈希的对手，还需要输入合法性校验（`ls_shadow_validate_input`，会给 `acceptInput` 增加原生调用延迟且必须进关键路径），属于另一个量级的改动，建议单独排期。

## 11. 已知取舍

**进程内加载 vs 旁路进程。** 进程内 FFM 的代价是原生崩溃会带走整个 JVM 及其上的 16 局对局。v1 建议接受该风险，靠 `enabled` 开关、`required = false` 的按房间 fail-open、以及严格的 ABI 契约控制，灰度期先在专用节点池开启。

若崩溃率不可接受，v2 改为旁路进程：每节点一个 `lockstep-shadow-host` 子进程，通过共享内存环加 Unix domain socket / named pipe 通信。房间侧 `ShadowVerifier` 接口不变，影子线程改为「写共享内存环并读结果」，内存模型改为「per-room 环形槽位」，房间线程的成本依然是一次投递。这是把 `ShadowVerifier` 立为边界接口的主要原因。

**预热池是否提前创建影子实例。** `preheat-ready-rooms = true` 可消除激活期的实例创建延迟，代价是空闲房间也占用原生内存。默认关闭；由于实例创建在 `activate` 阶段发起，而玩家连接通常需要数秒，绝大多数情况下第 1 帧到达前实例已就绪。

## 12. 配置

```yaml
lockstep:
  shadow:
    enabled: false                    # 默认关闭，灰度开启
    required: false                   # true 时加载失败拒绝启动且 readiness DOWN
    library-path: ${LOCKSTEP_SHADOW_LIBRARY:}
    expected-abi-version: 1
    worker-threads: 4
    checkpoint-interval-frames: 10
    pending-ring-depth: 4
    max-backlog-frames: 40            # 必须等于 pending-ring-depth × checkpoint-interval-frames
    max-frame-bytes: 16384
    step-budget: 8ms
    create-timeout: 5s
    native-memory-budget-per-room: 4MiB
    preheat-ready-rooms: false
    mismatch-policy: kick-player      # log-only | kick-player | terminate
    mismatch-streak-threshold: 2
```

## 13. 指标与健康

- `lockstep.shadow.library.loaded`：库加载状态与 build-id 标签
- `lockstep.shadow.instances{state}`：按 `ShadowState` 分组的实例数
- `lockstep.shadow.backlog.frames`：最大房间积压帧数
- `lockstep.shadow.step.duration`：带百分位直方图
- `lockstep.shadow.checkpoints{result=match|mismatch|missing}`
- `lockstep.shadow.degradations{reason}`
- `lockstep.shadow.native.bytes`：`ls_shadow_footprint` 汇总

`required = true` 时把库加载状态并入 `lockstepReadiness`。

## 14. 落地顺序

1. 扩展 `.proto` 并重新生成兼容性测试向量。
2. 建立 `shadow` 包骨架与 `ShadowVerifier` 假实现，先接通 `GameRoom` 的三个挂载点（`runTick` 投递、`onAuthoritativeHashOnLoop` 判定、`terminateOnLoop` 中的 `release`），纯 Java 可测。
3. 实现 `ShadowLibrary` 与 FFM 下行绑定，配一个自研 stub 库（对 payload 做 FNV-1a 累加）完成端到端联调，不阻塞于客户端交付。
4. 接入真实客户端导出库，跑「测试客户端 + 服务端影子」双跑一致性用例。
5. 补齐指标、健康、判定策略；同步更新 [连接路由与线程模型](threading-model.md) 的线程表与 [数据面协议](protocol.md)。

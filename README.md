# Rainnov Lockstep Server

单 JVM、引擎无关的帧同步房间节点。控制面使用 Spring WebFlux JSON API，
数据面使用 Netty RFC 6455 WebSocket 和 Protobuf 3。服务端只排序、补齐并广播
玩家输入，不运行游戏模拟，也不依赖 UE、Unity、Cocos 或其他引擎对象。

## 运行

要求 Java 25。开发环境可直接运行：

```powershell
$env:LOCKSTEP_NODE_ID = "local-node"
$env:LOCKSTEP_API_KEY = "replace-with-a-local-api-key"
$env:LOCKSTEP_TICKET_SECRET = "replace-with-a-long-random-secret"
.\gradlew.bat bootRun
```

默认监听：

- 控制面：`http://localhost:8080`
- 数据面：`ws://localhost:9000/game`
- WebSocket 子协议：`lockstep.protobuf.v1`

以下三个变量没有默认值，启动前必须设置：

```text
LOCKSTEP_NODE_ID
LOCKSTEP_API_KEY
LOCKSTEP_TICKET_SECRET
```

部署时还应按实际网络显式设置：

```text
LOCKSTEP_ADVERTISED_URI
LOCKSTEP_CONTROL_PORT
LOCKSTEP_DATA_PORT
```

应用不为节点 ID、API Key 或票据签名密钥提供默认值，缺少任一变量都会拒绝
启动。
加入票据必须放在首个 Protobuf `ClientHello` 内，不能放入 URL，也不应写入
日志。

节点内置数据面当前监听明文 `ws://`。生产返回 `wss://` 时，应由负载均衡器
或自研 Proxy 终止 TLS，再转发到节点的 `ws://` 端口；`LOCKSTEP_ADVERTISED_URI`
必须填写客户端实际访问的外部地址。首版节点本身不加载证书或终止 TLS。

## 生命周期与容量

节点按 `STARTING → READY → DRAINING → TERMINATED` 运行。数据面端口成功绑定且
初始房间池全部创建后，节点才进入 `READY`。

默认池中有 16 个一次性逻辑房间。这个数字代表所有未终止房间的总数，而不是
空闲房间目标数，因此分配一局不会立即扩容，单节点最多并发 16 局。房间完成
`TERMINATED` 后，运行对象、连接和帧历史被销毁；协调器随后创建具有全新
`roomId` 的 `READY` 房间补位。

```text
INITIALIZING → READY → ACTIVATING → ACTIVE → TERMINATING → TERMINATED
                                      └────→ FAILED ──────┘
```

每个房间固定绑定一个 Netty `EventExecutor`。REST 线程和网络线程只提交命令，
房间状态、玩家连接、输入聚合及 tick 都在该执行器上串行更新。节点关闭时先
进入 `DRAINING` 并拒绝新分配，最多等待活动对局 30 秒，然后强制结束；排空期
间不会补池。

## 接口与协议

- [控制面 API](docs/control-plane.md)
- [数据面协议、心跳、重连和兼容规则](docs/protocol.md)
- [Protobuf schema](src/main/proto/lockstep_v1.proto)

客户端认证后应严格每 5 秒发送一次 `ClientPing`，无论期间是否发送了输入。
服务端仅以相同 sequence 的 `ServerPong` 响应，不主动发起应用层 Ping。任一
已认证、结构有效的客户端消息都会刷新活动时间；15 秒没有此类消息时连接被
关闭，随后进入独立的 30 秒重连宽限。

## 健康与指标

- `/actuator/health/liveness`
- `/actuator/health/readiness`
- `/actuator/metrics`（要求 `X-API-Key`）

readiness 同时依赖节点状态、Netty 数据面和房间池。Micrometer 暴露房间状态、
健康房间数、分配成功/容量耗尽及异常终止等 `lockstep.*` 指标。

## 测试

```powershell
.\gradlew.bat test
```

测试覆盖房间一次性生命周期、并发容量、终止补位、输入窗口/no-op、票据签名
与过期、会话接管、历史回放、心跳/重连超时、WebSocket/Protobuf 边界及控制
面鉴权和幂等行为。

## 扩展边界

首版不实现匹配队列、持久化、游戏结果存储、引擎 SDK 或外部编排适配层。
横向扩展时由自研 Proxy 汇总多个独立节点的 capacity、持有全局幂等账本并
转发原始 `Idempotency-Key`；节点仍对最终分配进行原子容量检查，容量不足时
Proxy 改选其他节点。

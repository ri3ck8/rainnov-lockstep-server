# 帧同步测试客户端

测试客户端是一个独立 Gradle 源集中的命令行工具，用于通过真实控制面 HTTP
接口和数据面 WebSocket 连接验证多客户端帧同步效果。它不进入服务端生产制品，
入口类位于
`src/testClient/java/com/rainnov/lockstep/testclient/LockstepTestClient.java`。

## 验证内容

一次运行会自动完成以下操作：

1. 向控制面申请一个一次性房间及玩家票据。
2. 为每位玩家建立使用 `lockstep.protobuf.v1` 子协议的 WebSocket 连接。
3. 等待所有玩家加入并启动对局。
4. 每个客户端根据收到的权威帧，提前 `maxLeadFrames` 帧发送确定性整数移动输入。
5. 在预热结束后逐帧比较所有客户端收到的完整 `ServerFrame` 字节。
6. 每个客户端独立应用输入，逐帧比较本地模拟状态 SHA-256。
7. 检查验证区间内是否存在空操作，并统计实际帧接收间隔。
8. 无论成功或失败，都尽力通过控制面终止测试房间并关闭连接。

通过条件为：

- 所有客户端收到连续、编号相同且内容完全相同的权威帧；
- 所有客户端逐帧计算出的本地状态哈希相同；
- 验证区间内每位玩家的输入都已进入目标帧，不存在空操作；
- 运行期间未收到协议错误、非预期断线或对局提前结束事件。

本地模拟仅使用整数坐标和固定二进制输入格式，目的是排除浮点数及引擎差异，
直接验证相同输入流能否产生相同状态。服务端仍然只负责输入排序、补齐和广播，
不会执行这段模拟。

## 运行准备

先按项目部署配置启动服务端。测试客户端至少需要控制面地址和 API Key；数据面
地址默认取房间分配响应中的第一个 Protobuf WebSocket 端点。

```powershell
$env:LOCKSTEP_API_KEY = "replace-with-the-running-server-api-key"
$env:LOCKSTEP_TEST_CONTROL_URL = "http://localhost:8080"
.\gradlew.bat runFrameSyncClient
```

默认启动 2 个客户端并验证 120 个有效输入帧。在默认 20 Hz 配置下，加上预热帧
后通常需要约 6 秒。

如需临时覆盖参数，可使用 `testClientArgs`：

```powershell
.\gradlew.bat runFrameSyncClient `
  '-PtestClientArgs=--players=4 --frames=200 --timeout-seconds=15'
```

参数值不能包含空格。API Key 推荐通过环境变量传入，避免出现在进程命令行中。

## 参数

| 参数 | 环境变量 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `--control-url` | `LOCKSTEP_TEST_CONTROL_URL` | `http://localhost:8080` | 控制面根地址 |
| `--api-key` | `LOCKSTEP_API_KEY` | 无 | 控制面 API Key |
| `--players` | `LOCKSTEP_TEST_PLAYERS` | `2` | 模拟玩家数，不能超过服务端房间上限 |
| `--frames` | `LOCKSTEP_TEST_FRAMES` | `120` | 预热后需要验证的有效输入帧数 |
| `--timeout-seconds` | `LOCKSTEP_TEST_TIMEOUT_SECONDS` | `10` | 单条 WebSocket 或控制面响应等待上限 |
| `--data-uri` | `LOCKSTEP_TEST_DATA_URI` | 分配响应端点 | 强制使用指定的 `ws://` 或 `wss://` 地址 |
| `--match-id` | `LOCKSTEP_TEST_MATCH_ID` | 自动生成 | 指定本次测试的对局 ID |

命令行参数的优先级高于环境变量。查看内置帮助：

```powershell
.\gradlew.bat runFrameSyncClient '-PtestClientArgs=--help'
```

当服务端返回的是集群外部地址，而测试客户端需要直连节点本地端口时，可覆盖数据
面地址：

```powershell
$env:LOCKSTEP_TEST_DATA_URI = "ws://127.0.0.1:9000/game"
.\gradlew.bat runFrameSyncClient
```

## 结果解读

成功结果会以 `[通过]` 开头，并包含：

- 验证帧范围和数量；
- 有效输入与空操作数量；
- 整段权威帧流的 SHA-256；
- 最终本地状态的 SHA-256 和各玩家整数坐标；
- 所有客户端合并统计的平均、P95、最大帧接收间隔及理论间隔。

权威帧流哈希包含动态对局输入和帧号，用于同一次运行中客户端之间的比较，不是
跨运行固定的兼容性向量。协议二进制兼容性仍由
`src/test/resources/protocol-v1` 下的标准向量验证。

失败时进程返回非零退出码。常见原因包括：

- `申请房间失败：HTTP 401/403`：API Key 与服务端不一致；
- 没有可用房间或玩家数超过容量：调整房间池或玩家数量；
- WebSocket 连接失败：检查分配响应中的数据面地址，必要时使用 `--data-uri`；
- `INVALID_TARGET_FRAME` 或验证帧出现空操作：客户端到节点的时延超过当前
  `maxLeadFrames` 提供的输入提前量，或节点负载过高；
- 帧号不连续、权威帧不一致或状态哈希不一致：测试直接失败，应保留完整输出并
  检查对应帧的数据面日志。

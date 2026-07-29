# 控制面接口 v1

所有 `/api/v1/**` 和 `/internal/v1/**` 请求都要求：

```http
X-API-Key: <configured key>
```

服务端接受可选的 `X-Request-Id`（最长 128 字符），并始终在响应头回传最终请求 ID。错误体统一为：

```json
{
  "code": "INVALID_REQUEST",
  "message": "players must not be empty",
  "requestId": "72b8b697-950d-4a5e-8eae-6da7801f8822",
  "timestamp": "2026-07-24T10:00:00Z"
}
```

## 申请房间

```http
POST /api/v1/room-allocations
Idempotency-Key: match-request-10001
Content-Type: application/json
```

```json
{
  "matchId": "match-10001",
  "players": [
    {"playerId": "player-a"},
    {"playerId": "player-b"}
  ]
}
```

玩家顺序是该局永久的帧聚合顺序。首次成功返回 `201 Created`；相同幂等键与相同请求重放返回 `200 OK` 和同一分配记录、房间及票据。相同键绑定不同请求返回 `409 Conflict`。

响应示例：

```json
{
  "allocationId": "alloc-...",
  "nodeId": "node-sg-1",
  "roomId": "room-...",
  "matchId": "match-10001",
  "roomStatus": "ACTIVE",
  "matchPhase": "WAITING_FOR_PLAYERS",
  "protocolVersion": 1,
  "tickRate": 20,
  "inputDelayFrames": 2,
  "maxLeadFrames": 4,
  "clientPingIntervalMillis": 5000,
  "connectionIdleTimeoutMillis": 15000,
  "reconnectGraceMillis": 30000,
  "joinDeadline": "2026-07-24T10:01:00Z",
  "players": [
    {
      "playerId": "player-a",
      "ticket": "<opaque HMAC ticket>",
      "expiresAt": "2026-07-24T11:01:30Z"
    }
  ],
  "dataPlaneEndpoints": [
    {
      "transport": "WEBSOCKET",
      "uri": "wss://games.example.com/game",
      "subprotocol": "lockstep.protobuf.v1",
      "encoding": "PROTOBUF"
    }
  ]
}
```

没有 `READY` 房间或节点正在排空时立即返回 `503 Service Unavailable` 和 `Retry-After`；节点内部不排队。

示例中的 `wss://` 是客户端看到的 TLS 终止层地址。节点原生 Netty 端口使用 `ws://`；负载均衡器或自研 Proxy 负责 TLS，并将连接转发到节点。

## 查询房间

```http
GET /api/v1/rooms/{roomId}
```

返回房间/对局状态、玩家连接状态、当前帧、生命周期时间及终止原因。响应不包含加入票据。终止后的不可变快照保留 10 分钟。

## 终止对局

```http
POST /api/v1/rooms/{roomId}/termination
Content-Type: application/json
```

正常结束：

```json
{
  "matchId": "match-10001",
  "mode": "GRACEFUL",
  "reason": "MATCH_COMPLETED"
}
```

管理强制结束可使用 `mode=FORCE`、`reason=ADMINISTRATIVE`。重复提交会返回同一终态快照；`matchId` 与房间不匹配时返回 `409 Conflict`。

## 节点容量

```http
GET /internal/v1/node/capacity
```

返回稳定的 `nodeId`、`nodeStatus`、数据面端点、`acceptingAllocations`、目标容量，以及 `initializingRooms`、`readyRooms`、`activatingRooms`、`activeRooms`、`failedRooms`、`terminatingRooms`、`healthyRooms`、`totalLiveRooms` 和 `sampledAt`。调用方必须直接使用 `acceptingAllocations` 判断节点当前是否接受申请，不应根据 `nodeStatus` 或房间计数推断。

未来 Proxy 可以短时缓存该快照，但它不是分配承诺；实际申请返回 503 时必须改选其他节点。节点只保证本节点内的 `matchId` 和幂等键唯一。

## 节点存活房间列表

```http
GET /internal/v1/node/rooms
```

返回当前节点的全部未终止房间快照，不分页，也不包含已进入终态快照保留区的房间：

```json
{
  "nodeId": "node-sg-1",
  "items": [
    {
      "nodeId": "node-sg-1",
      "roomId": "room-...",
      "allocationId": "alloc-...",
      "matchId": "match-10001",
      "state": "ACTIVE",
      "matchPhase": "WAITING_FOR_PLAYERS",
      "currentFrame": 0,
      "players": [
        {
          "playerId": "player-a",
          "state": "RESERVED",
          "sessionId": null,
          "lastInputFrame": 0,
          "lastSequence": 0
        }
      ],
      "createdAt": "2026-07-24T10:00:00Z",
      "activatedAt": "2026-07-24T10:00:01Z",
      "startedAt": null,
      "joinDeadline": "2026-07-24T10:01:01Z",
      "terminatedAt": null,
      "terminationMode": null,
      "terminationReason": null,
      "lastTickLagNanos": 0
    }
  ],
  "total": 1,
  "sampledAt": "2026-07-24T10:00:02Z"
}
```

`total` 始终等于 `items` 的元素数量。列表使用与单房间查询相同、且不含连接票据的 `RoomSnapshot` 结构；需要查询已终止房间时继续使用 `GET /api/v1/rooms/{roomId}`。

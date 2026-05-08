# GaoXinLibrary.RabbitMQ

RabbitMQ 简化封装库，基于 `RabbitMQ.Client 7.x`，支持 .NET 8/9/10。

## 安装

```bash
dotnet add package GaoXinLibrary.RabbitMQ
```

## 特性

- **发布/订阅模式** — 实现 `IMessageHandler<T>` + `[RabbitMQSubscribe]` 即可自动消费
- **延迟队列** — 基于死信交换机 + 消息 TTL，无需安装插件
- **交换机类型** — Direct / Fanout / Topic / Headers 全支持
- **消息优先级** — 队列级 `x-max-priority` + 消息级 `Priority` 支持
- **死信队列** — 重试耗尽后自动投递到 `{queue}.dead`，不丢失消息
- **消息追踪** — 自动生成 `MessageId` + `Timestamp`，全链路日志可追踪
- **队列最大长度** — `x-max-length` 防止消息堆积
- **Publisher Confirms** — 发布者确认模式，保障消息可靠投递
- **DI 注入** — `AddRabbitMQ()` + `AddRabbitMQHandlers()` 自动扫描注册
- **批量发布** — `PublishBatchAsync` 批量投递消息，高吞吐场景性能更优
- **连接自动重连** — 连接断开后指数退避自动重连，支持配置最大重试次数
- **消费者 Channel 自动恢复** — 后台定期检测 Channel 状态，断开后自动重建消费者
- **健康检查** — 集成 ASP.NET Core `IHealthCheck`，支持 `/health` 端点探活
- **配置验证** — 注册时即刻校验必填项，Fail-Fast 避免运行时才报错

## 快速开始

### 1. 注册服务

```csharp
builder.Services.AddRabbitMQ(options =>
{
    options.HostName = "localhost";
    options.UserName = "guest";
    options.Password = "guest";
});

// 自动扫描当前程序集中所有标注了 [RabbitMQSubscribe] 的 Handler
builder.Services.AddRabbitMQHandlers();

// 也可指定程序集扫描
// builder.Services.AddRabbitMQHandlers(typeof(OrderCreatedHandler).Assembly);

// 或手动注册单个 Handler
// builder.Services.AddRabbitMQHandler<OrderCreatedHandler>();
```

### 2. 定义消息

```csharp
public class OrderCreatedEvent
{
    public string OrderId { get; set; } = "";
    public decimal Amount { get; set; }
}
```

### 3. 发布消息

```csharp
public class OrderService
{
    private readonly IRabbitMQPublisher _publisher;

    public OrderService(IRabbitMQPublisher publisher) => _publisher = publisher;

    public async Task CreateOrderAsync()
    {
        // 立即发布
        await _publisher.PublishAsync("order.exchange", "order.created",
            new OrderCreatedEvent { OrderId = "123", Amount = 99.9m });

        // 延迟 30 秒投递
        await _publisher.PublishAsync("order.exchange", "order.timeout",
            new OrderCreatedEvent { OrderId = "123" },
            delay: TimeSpan.FromSeconds(30));

        // 带自定义 Headers
        await _publisher.PublishAsync("notify.exchange", "notify",
            new OrderCreatedEvent { OrderId = "123" },
            headers: new Dictionary<string, object?> { ["type"] = "order" });

        // 带优先级（需队列声明 MaxPriority）
        await _publisher.PublishAsync("order.exchange", "order.vip",
            new OrderCreatedEvent { OrderId = "456", Amount = 999m },
            priority: 9);

        // 批量发布（高吞吐场景推荐）
        var orders = Enumerable.Range(1, 100).Select(i =>
            new OrderCreatedEvent { OrderId = $"batch_{i}", Amount = i * 10m });
        await _publisher.PublishBatchAsync("order.exchange", "order.created", orders);
    }
}
```

### 4. 消费消息

```csharp
// Topic 交换机
[RabbitMQSubscribe("order.exchange", "order.*", RabbitMQExchangeType.Topic)]
public class OrderCreatedHandler : IMessageHandler<OrderCreatedEvent>
{
    public Task HandleAsync(OrderCreatedEvent message, CancellationToken ct = default)
    {
        Console.WriteLine($"收到订单：{message.OrderId}");
        return Task.CompletedTask;
    }
}

// Fanout 广播
[RabbitMQSubscribe("broadcast.exchange", exchangeType: RabbitMQExchangeType.Fanout)]
public class BroadcastHandler : IMessageHandler<OrderCreatedEvent>
{
    public Task HandleAsync(OrderCreatedEvent message, CancellationToken ct = default)
    {
        Console.WriteLine($"广播消息：{message.OrderId}");
        return Task.CompletedTask;
    }
}

// Headers 交换机
[RabbitMQSubscribe("header.exchange", exchangeType: RabbitMQExchangeType.Headers,
    MatchHeaders = new[] { "type=order", "region=cn" }, HeaderMatchType = "all")]
public class HeaderMatchHandler : IMessageHandler<OrderCreatedEvent>
{
    public Task HandleAsync(OrderCreatedEvent message, CancellationToken ct = default)
    {
        Console.WriteLine($"Headers 匹配消息：{message.OrderId}");
        return Task.CompletedTask;
    }
}
```

## 延迟队列原理

```
Publisher → [exchange.delay] → [exchange.routingKey.delay queue (TTL)] → 过期 → [exchange] → Consumer
```

基于死信交换机（DLX）+ 消息级别 TTL 实现，无需安装 `rabbitmq_delayed_message_exchange` 插件。

## 交换机类型

| 类型 | 枚举值 | 路由规则 | 适用场景 |
|------|--------|----------|----------|
| Direct | `RabbitMQExchangeType.Direct` | RoutingKey 精确匹配 | 点对点任务分发、精确路由 |
| Fanout | `RabbitMQExchangeType.Fanout` | 忽略 RoutingKey，广播到所有绑定队列 | 广播通知、多服务同步消费 |
| Topic | `RabbitMQExchangeType.Topic` | RoutingKey 通配符匹配（`*` 匹配一段，`#` 匹配多段） | 按类别分发，如 `order.*`、`log.#` |
| Headers | `RabbitMQExchangeType.Headers` | 根据消息 Headers 属性匹配，忽略 RoutingKey | 多维度条件路由，如按地区 + 类型过滤 |

**Topic 通配符示例：**

```
RoutingKey: order.created  → 匹配 order.*、order.#
RoutingKey: order.cn.vip   → 匹配 order.#，不匹配 order.*
```

**Headers 匹配示例：**

```csharp
// 需同时满足 type=order AND region=cn（all 模式）
[RabbitMQSubscribe("header.exchange", exchangeType: RabbitMQExchangeType.Headers,
    MatchHeaders = new[] { "type=order", "region=cn" }, HeaderMatchType = "all")]

// 满足 type=order OR region=cn 任意一个即可（any 模式）
[RabbitMQSubscribe("header.exchange", exchangeType: RabbitMQExchangeType.Headers,
    MatchHeaders = new[] { "type=order", "region=cn" }, HeaderMatchType = "any")]
```

发布时通过 `headers` 参数携带匹配字段：

```csharp
await publisher.PublishAsync("header.exchange", "",
    new NotifyEvent { Content = "测试" },
    headers: new Dictionary<string, object?> { ["type"] = "order", ["region"] = "cn" });
```

## 消费重试策略

消息处理失败时自动进入重试流程：

```
消费失败 → 即时重试（MaxRetries 次） → 延迟重试（MaxDelayRetries 次，支持固定延迟/指数退避+抖动） → 死信队列 / 丢弃
```

**流程细节：**

1. Handler 抛异常 → 先 republish 到目标队列（即时重试/延迟重试）→ **republish 成功后再 ACK 原消息**
2. 即时重试耗尽 → 发布到 `{queue}.retry.wait` 队列（带 TTL）
3. TTL 过期 → 消息死信回原队列（`x-retry-count` 重置，`x-delay-retry-count + 1`）
4. 延迟重试也耗尽 → 投递到 `{queue}.dead` 死信队列（启用时）或 ACK 丢弃（禁用时）

**全局配置**（`RabbitMQOptions`）：

```csharp
builder.Services.AddRabbitMQ(options =>
{
    options.MaxRetries = 3;              // 即时重试 3 次
    options.MaxDelayRetries = 3;         // 延迟重试 3 次
    options.RetryDelaySeconds = 10;      // 基础延迟 10 秒
    options.EnableExponentialRetryBackoff = true; // 可选：指数退避
    options.MaxRetryDelaySeconds = 300;  // 延迟上限 300 秒
    options.EnableRetryJitter = true;    // 可选：延迟抖动
    options.EnableDeadLetter = true;     // 启用死信队列（默认 true）
    options.EnablePublisherConfirms = false; // 发布者确认（默认 false）
});
```

**单个 Handler 覆盖**（`-1` 使用全局配置）：

```csharp
[RabbitMQSubscribe("order.exchange", "order.*", RabbitMQExchangeType.Topic,
    MaxRetries = 5, MaxDelayRetries = 2, RetryDelaySeconds = 30,
    MaxPriority = 10, MaxLength = 10000, EnableDeadLetter = TriState.Enabled)]
public class OrderHandler : IMessageHandler<OrderEvent> { ... }
```

## 消息优先级

通过 `[RabbitMQSubscribe]` 的 `MaxPriority` 声明队列支持优先级，然后发布时指定 `priority` 参数：

```csharp
// 声明队列支持优先级 0-10
[RabbitMQSubscribe("order.exchange", "order.*", RabbitMQExchangeType.Topic, MaxPriority = 10)]
public class PriorityOrderHandler : IMessageHandler<OrderEvent> { ... }

// 发布时指定优先级
await publisher.PublishAsync("order.exchange", "order.vip",
    new OrderEvent { OrderId = "VIP001" }, priority: 9);
```

> **注意**：`MaxPriority` 建议设置为 1-10，值越大队列内存开销越高。发布时 `priority` 不应超过队列的 `MaxPriority`。

## 死信队列

当所有重试耗尽后，消息不再丢弃，而是投递到 `{queue}.dead` 死信队列：

```
重试耗尽 → {queue}.dead（带 x-dead-reason / x-dead-handler / x-dead-time 头信息）
```

- 全局配置：`options.EnableDeadLetter = true`（默认启用）
- 单个 Handler 覆盖：`EnableDeadLetter = TriState.Enabled`（启用）或 `TriState.Disabled`（禁用），`TriState.Default`（使用全局配置）

## 消费者预取数（PrefetchCount）

`PrefetchCount` 控制 RabbitMQ 一次最多向消费者推送多少条未确认（Unacked）消息，是消费者流量控制的核心参数。

**工作原理：**

```
消费者向 RabbitMQ 声明：我最多同时处理 N 条消息
→ RabbitMQ 最多推送 N 条到本地缓冲区
→ 每 ACK 一条，才会推送下一条
→ 防止消费者被大量消息压垮
```

**选择建议：**

| 场景 | 推荐值 | 原因 |
|------|--------|------|
| 快速处理（无 I/O，纯计算） | 10–50 | 提高吞吐量 |
| 中等耗时（数据库、HTTP 调用） | 5–10 | 平衡吞吐与内存 |
| 慢处理（长时间任务） | 1–3 | 避免消息堆积在本地 |
| 优先级队列 | **1** | 确保 RabbitMQ 能按优先级顺序投递，值大于 1 时高优先级消息可能被低优先级占位 |

```csharp
// 全局配置
builder.Services.AddRabbitMQ(options =>
{
    options.PrefetchCount = 10; // 全局默认
});

// 单个 Handler 覆盖（优先级队列建议设为 1）
[RabbitMQSubscribe("priority.exchange", "priority.order",
    MaxPriority = 10, PrefetchCount = 1)]
public class PriorityHandler : IMessageHandler<OrderEvent> { ... }
```

## 发布者确认模式（Publisher Confirms）

### 什么是 Publisher Confirms？

RabbitMQ 默认情况下，`PublishAsync` 调用成功只代表消息已写入网络缓冲区，并**不保证** Broker 已经收到并持久化。若 Broker 在写入磁盘前崩溃，消息会永久丢失。

启用 Publisher Confirms 后，RabbitMQ 会在消息**成功路由并持久化到队列**后，向发布者返回一个 `ack` 确认；若路由失败或磁盘写入失败则返回 `nack`。发布者等待该确认后才算投递成功，从根本上解决"发后即忘"导致的消息丢失问题。

### 适合启用的场景

- 支付、订单、资金流水等**不允许消息丢失**的核心业务
- 跨服务的**强一致性**事件通知
- 需要**审计追踪**的操作日志

### 不建议启用的场景

- 日志采集、监控指标等**允许少量丢失**的高频消息（性能敏感）
- 单机开发/测试环境（无需额外保障）

### 性能影响

Publisher Confirms 采用**同步等待**模式，每次 `PublishAsync` 都会阻塞直到收到 Broker 的 ack/nack，因此会显著降低发布吞吐量：

| 模式 | 吞吐量（参考） | 可靠性 |
|------|--------------|--------|
| 不启用（默认） | 高（fire-and-forget） | 消息可能在 Broker 崩溃时丢失 |
| 启用 Publisher Confirms | 低（等待 ack） | Broker 持久化后才返回，可靠性高 |

> 如需兼顾高吞吐与可靠性，可结合**业务侧幂等 + 本地消息表**方案，不必强依赖 Publisher Confirms。

### 启用方式

```csharp
builder.Services.AddRabbitMQ(options =>
{
    options.EnablePublisherConfirms = true; // 开启发布者确认
});
```

启用后，`PublishAsync` 会在 Broker 未确认或发布失败时抛出异常，调用方可捕获后进行补偿（如写入本地重试表）：

```csharp
try
{
    await publisher.PublishAsync("order.exchange", "order.created", orderEvent);
}
catch (Exception ex)
{
    // Broker 未确认，记录到本地重试表或告警
    logger.LogError(ex, "消息发布失败，OrderId={OrderId}", orderEvent.OrderId);
}
```

## 消息追踪

每条消息自动生成 `MessageId`（GUID）和 `Timestamp`（Unix 时间戳），日志中全链路输出：

```
[Handler] 消息处理成功，MessageId=abc123
[Handler] 处理失败，即时重试 1/3，MessageId=abc123
[Handler] 消息已投递到死信队列，MessageId=abc123
```

重试过程中 MessageId 保持不变，方便端到端追踪。

## 配置项

| 属性 | 默认值 | 说明 |
|------|--------|------|
| HostName | localhost | RabbitMQ 主机 |
| Port | 5672 | 端口 |
| UserName | guest | 用户名 |
| Password | guest | 密码 |
| VirtualHost | / | 虚拟主机 |
| PrefetchCount | 10 | 消费者预取数 |
| MaxRetries | 3 | 即时重试最大次数 |
| MaxDelayRetries | 3 | 延迟重试最大次数 |
| RetryDelaySeconds | 10 | 延迟重试间隔秒数 |
| EnableExponentialRetryBackoff | false | 是否启用指数退避延迟重试 |
| MaxRetryDelaySeconds | 300 | 延迟重试最大间隔秒数 |
| EnableRetryJitter | true | 是否在延迟重试中加入随机抖动 |
| EnableDeadLetter | true | 是否启用死信队列 |
| EnablePublisherConfirms | false | 是否启用发布者确认模式 |
| AutoMigrateQueues | false | 队列参数不匹配时是否自动删除并重建队列（默认关闭，避免生产误删） |
| ShutdownTimeoutSeconds | 30 | 优雅关闭超时秒数（等待正在处理的消息完成） |
| HealthCheckTimeoutSeconds | 2 | 健康检查连接超时秒数 |
| AutomaticRecoveryEnabled | true | 是否启用 RabbitMQ.Client 内置的自动恢复 |
| NetworkRecoveryInterval | 5s | 自动恢复间隔 |
| ReconnectMaxRetries | 10 | 连接断开后重连最大重试次数（0=不限次数） |
| ReconnectInitialDelay | 1s | 重连初始延迟（指数退避） |
| ReconnectMaxDelay | 30s | 重连最大延迟 |

### RabbitMQSubscribeAttribute 参数

| 属性 | 默认值 | 说明 |
|------|--------|------|
| Exchange | — | 交换机名称（必填） |
| RoutingKey | "" | 路由键 |
| ExchangeType | Direct | 交换机类型 |
| Queue | null | 自定义队列名，空则自动生成 |
| Durable | true | 是否持久化 |
| AutoDelete | false | 是否自动删除 |
| MaxPriority | 0 | 队列最大优先级（0=不启用，建议 1-10） |
| MaxLength | 0 | 队列最大长度（0=不限制） |
| EnableDeadLetter | TriState.Default | 是否启用死信队列（`Default`=全局，`Enabled`=启用，`Disabled`=禁用） |
| MaxRetries | -1 | 即时重试次数（-1=全局） |
| MaxDelayRetries | -1 | 延迟重试次数（-1=全局） |
| RetryDelaySeconds | -1 | 延迟重试间隔（-1=全局） |
| PrefetchCount | -1 | 消费者预取数（-1=全局，优先级队列建议设为 1） |
| HeaderMatchType | "all" | Headers 交换机匹配模式：`all`（全部满足）或 `any`（任意满足） |
| MatchHeaders | null | Headers 匹配条件，格式 `"key=value"`，仅 Headers 交换机有效 |

## 常见问题

**Q：消息发布后消费者没有收到？**

- 确认交换机和队列已正确声明（Handler 启动时自动声明，需先启动消费者服务）
- 检查 `RoutingKey` 是否与消费者绑定的路由键匹配
- Fanout 交换机无需 `RoutingKey`，Topic 交换机注意通配符 `*`/`#` 的区别

**Q：重试后消息消失了，没有进入死信队列？**

- 检查全局配置或 Handler 的 `EnableDeadLetter` 是否为 `true`/`TriState.Enabled`
- 死信队列名为 `{queue}.dead`，可在 RabbitMQ 管理界面中确认该队列已创建

**Q：优先级队列不按优先级消费？**

- 将消费者的 `PrefetchCount` 设为 `1`，否则 RabbitMQ 会一次性推送多条消息到本地缓冲区，导致优先级排序失效
- 确保发布时传入的 `priority` 值不超过队列声明的 `MaxPriority`

**Q：延迟消息延迟时间不准确？**

- 本库延迟队列基于消息级别 TTL + 死信交换机实现，TTL 精度受 RabbitMQ 内部定时器影响，误差通常在 1 秒以内
- 极短延迟（< 1 秒）不建议使用此方案

**Q：何时应该启用 `EnablePublisherConfirms`？**

- 业务对消息零丢失有强要求时启用（如支付、订单）
- 对吞吐量敏感的场景（如日志、监控）应保持默认关闭，可通过业务幂等 + 本地消息表替代

## 批量发布

高吞吐场景下，逐条 `PublishAsync` 性能较低。`PublishBatchAsync` 复用同一个 Channel 连续发布多条消息，减少网络往返开销：

```csharp
var events = new List<OrderCreatedEvent>
{
    new() { OrderId = "001", Amount = 100m },
    new() { OrderId = "002", Amount = 200m },
    new() { OrderId = "003", Amount = 300m }
};

await publisher.PublishBatchAsync("order.exchange", "order.created", events);

// 也支持共用 headers 和 priority
await publisher.PublishBatchAsync("order.exchange", "order.vip", events,
    headers: new Dictionary<string, object?> { ["source"] = "batch" },
    priority: 5);
```

> **注意**：`PublishBatchAsync` 不支持延迟投递（`delay`），如需延迟请逐条使用 `PublishAsync`。

## 连接自动重连

当 RabbitMQ 服务重启或网络中断时，SDK 会自动尝试重连，采用指数退避策略：

```
连接失败 → 等待 1s → 重试 → 等待 2s → 重试 → 等待 4s → ... → 最长等待 30s
```

**配置示例：**

```csharp
builder.Services.AddRabbitMQ(options =>
{
    options.HostName = "rabbitmq-server";
    options.UserName = "admin";
    options.Password = "password";

    // 连接恢复配置
    options.AutomaticRecoveryEnabled = true;     // 启用 RabbitMQ.Client 内置自动恢复（默认 true）
    options.NetworkRecoveryInterval = TimeSpan.FromSeconds(5); // 内置恢复间隔（默认 5s）

    // SDK 层重连配置（当 GetConnectionAsync 发现连接断开时触发）
    options.ReconnectMaxRetries = 10;            // 最大重试 10 次（0=不限次数）
    options.ReconnectInitialDelay = TimeSpan.FromSeconds(1);  // 首次重试等待 1s
    options.ReconnectMaxDelay = TimeSpan.FromSeconds(30);     // 单次最长等待 30s
});
```

重连过程中的日志输出：

```
WARN  RabbitMQ 连接失败（第 1 次），将在 1s 后重试
WARN  RabbitMQ 连接失败（第 2 次），将在 2s 后重试
INFO  RabbitMQ 连接已恢复（第 3 次重试）
```

> 若超过 `ReconnectMaxRetries` 次仍无法连接，将抛出异常并记录 `CRITICAL` 级别日志。

## 消费者 Channel 自动恢复

消费者启动后，SDK 在后台每 10 秒检测一次所有消费者 Channel 的状态。当检测到 Channel 已关闭（如 RabbitMQ 重启、网络断开）时，会自动重新获取连接、创建新 Channel 并重新注册消费者：

```
Channel 断开 → 清理旧 Channel → 获取连接（触发重连） → 创建新 Channel → 重新声明队列/绑定 → 恢复消费
```

恢复过程中的日志输出：

```
WARN  消费者 OrderHandler 的 Channel 已断开，正在尝试恢复...
INFO  消费者 OrderHandler 的 Channel 已恢复
```

> 无需任何额外配置，此功能默认启用。

## 健康检查

SDK 提供 `IHealthCheck` 实现，可集成到 ASP.NET Core 的健康检查端点，用于 K8s / Docker 存活探针检测：

```csharp
// 注册健康检查
builder.Services.AddRabbitMQ(options => { /* ... */ });
builder.Services.AddRabbitMQHealthCheck(); // 默认名称 "rabbitmq"

// 也可自定义名称和标签
builder.Services.AddRabbitMQHealthCheck("mq", "ready", "infrastructure");

// 映射健康检查端点
app.MapHealthChecks("/health");
```

健康检查会按 `HealthCheckTimeoutSeconds`（默认 2 秒）快速失败，避免连接重试退避拖慢探针。

**响应示例：**

```json
{
  "status": "Healthy",
  "entries": {
    "rabbitmq": {
      "status": "Healthy",
      "description": "RabbitMQ 连接正常"
    }
  }
}
```

## 幂等扩展点

库内提供 `IMessageDeduplicator` 扩展点，默认实现是 `NoOpMessageDeduplicator`（不改变现有行为）。  
如果需要 Redis/数据库去重，可直接覆盖注册：

```csharp
builder.Services.AddSingleton<IMessageDeduplicator, RedisMessageDeduplicator>();
```

消费者执行前会调用 `IsDuplicateAsync`，执行成功后调用 `MarkProcessedAsync`；重复消息会直接 ACK 并跳过处理。

## 指标（Metrics）

内置 `System.Diagnostics.Metrics` 指标（Meter：`GaoXinLibrary.RabbitMQ`）：

- `rabbitmq.consumer.processed`
- `rabbitmq.consumer.retry`
- `rabbitmq.consumer.delay_retry`
- `rabbitmq.consumer.dead_letter`
- `rabbitmq.consumer.discarded`
- `rabbitmq.consumer.duplicate`
- `rabbitmq.consumer.handle.duration.ms`
- `rabbitmq.consumer.inflight`

可通过 OpenTelemetry 或 `MeterListener` 直接采集。

## 配置验证

SDK 在 `AddRabbitMQ()` 注册时会立即校验必填配置项（Fail-Fast），而不是等到第一次连接时才报错：

```csharp
// ❌ 缺少必填项，注册时立即抛出 ValidationException
builder.Services.AddRabbitMQ(options =>
{
    options.HostName = "";  // 不能为空
});
// 抛出: ValidationException: "RabbitMQ HostName 不能为空"

// ❌ Port 超出范围
builder.Services.AddRabbitMQ(options =>
{
    options.Port = 99999;  // 超出 1-65535 范围
});
// 抛出: ValidationException: "RabbitMQ Port 必须在 1-65535 范围内"
```

| 属性 | 验证规则 |
|------|----------|
| HostName | 必填（`[Required]`） |
| Port | 范围 1-65535（`[Range]`） |
| UserName | 必填（`[Required]`） |
| Password | 必填（`[Required]`） |
| VirtualHost | 必填（`[Required]`） |

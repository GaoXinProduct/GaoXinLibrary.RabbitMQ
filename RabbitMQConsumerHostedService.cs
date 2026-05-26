using System.Reflection;
using System.Text.Json;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace GaoXinLibrary.RabbitMQ;

/// <summary>
/// 后台服务：自动发现并启动所有注册的消息处理器，支持即时重试 + 延迟重试策略
/// </summary>
internal sealed class RabbitMQConsumerHostedService : BackgroundService
{
    private const string HeaderRetryCount = "x-retry-count";
    private const string HeaderDelayRetryCount = "x-delay-retry-count";

    private readonly IServiceProvider _serviceProvider;
    private readonly RabbitMQConnectionManager _connectionManager;
    private readonly IEnumerable<MessageHandlerDescriptor> _descriptors;
    private readonly RabbitMQOptions _options;
    private readonly IMessageDeduplicator _deduplicator;
    private readonly ILogger<RabbitMQConsumerHostedService> _logger;
    private readonly List<ConsumerRegistration> _consumerRegistrations = [];
#if NET9_0_OR_GREATER
    private readonly System.Threading.Lock _consumerStateLock = new();
#else
    private readonly object _consumerStateLock = new();
#endif
    private readonly CancellationTokenSource _shutdownCts = new();
    private readonly ObservableGauge<int> _inflightGauge;
    private int _inflightCount;
    private static readonly Meter Meter = new("GaoXinLibrary.RabbitMQ");
    private static readonly Counter<long> ProcessedCounter = Meter.CreateCounter<long>("rabbitmq.consumer.processed");
    private static readonly Counter<long> RetryCounter = Meter.CreateCounter<long>("rabbitmq.consumer.retry");
    private static readonly Counter<long> DelayRetryCounter = Meter.CreateCounter<long>("rabbitmq.consumer.delay_retry");
    private static readonly Counter<long> DeadLetterCounter = Meter.CreateCounter<long>("rabbitmq.consumer.dead_letter");
    private static readonly Counter<long> DiscardCounter = Meter.CreateCounter<long>("rabbitmq.consumer.discarded");
    private static readonly Counter<long> DuplicateCounter = Meter.CreateCounter<long>("rabbitmq.consumer.duplicate");
    private static readonly Histogram<double> HandleDurationMs = Meter.CreateHistogram<double>("rabbitmq.consumer.handle.duration.ms");

    public RabbitMQConsumerHostedService(
        IServiceProvider serviceProvider,
        RabbitMQConnectionManager connectionManager,
        IEnumerable<MessageHandlerDescriptor> descriptors,
        IOptions<RabbitMQOptions> options,
        IMessageDeduplicator deduplicator,
        ILogger<RabbitMQConsumerHostedService> logger)
    {
        _serviceProvider = serviceProvider;
        _connectionManager = connectionManager;
        _descriptors = descriptors;
        _options = options.Value;
        _deduplicator = deduplicator;
        _logger = logger;
        _inflightGauge = Meter.CreateObservableGauge(
            "rabbitmq.consumer.inflight",
            () => Volatile.Read(ref _inflightCount),
            description: "Current in-flight RabbitMQ messages");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var connection = await _connectionManager.GetConnectionAsync(stoppingToken);
        var descriptorList = _descriptors.ToList();
        _logger.LogInformation("RabbitMQ consumer service started, {Count} handler(s) found: {Handlers}",
            descriptorList.Count, string.Join(", ", descriptorList.Select(d => d.HandlerType.Name)));

        foreach (var descriptor in descriptorList)
        {
            var attr = descriptor.HandlerType.GetCustomAttribute<RabbitMQSubscribeAttribute>();
            if (attr is null)
            {
                _logger.LogWarning("Handler {Handler} is missing [Subscribe] attribute, skipped.", descriptor.HandlerType.Name);
                continue;
            }

            try
            {
                await StartConsumerAsync(connection, descriptor, attr, stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to start consumer {Handler}.", descriptor.HandlerType.Name);
            }
        }

        // ── Channel 健康监控：检测断开并自动恢复 ─────────────────────────
        await MonitorChannelsAsync(stoppingToken);
    }

    private async Task MonitorChannelsAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            List<ConsumerRegistration> snapshot;
            lock (_consumerStateLock)
            {
                snapshot = [.. _consumerRegistrations];
            }

            foreach (var registration in snapshot)
            {
                if (registration.Channel.IsOpen)
                    continue;

                var handlerName = registration.Descriptor.HandlerType.Name;

                _logger.LogWarning("Consumer {Handler} channel disconnected, attempting recovery...", handlerName);

                try
                {
                    lock (_consumerStateLock)
                    {
                        _consumerRegistrations.Remove(registration);
                    }
                    try { registration.Channel.Dispose(); } catch { /* ignore */ }

                    var connection = await _connectionManager.GetConnectionAsync(stoppingToken);
                    await StartConsumerAsync(connection, registration.Descriptor, registration.Attr, stoppingToken);
                    _logger.LogInformation("Consumer {Handler} channel recovered", handlerName);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Consumer {Handler} channel recovery failed, will retry on next check", handlerName);
                }
            }
        }
    }

    private async Task StartConsumerAsync(
        IConnection connection, MessageHandlerDescriptor descriptor, RabbitMQSubscribeAttribute attr, CancellationToken ct)
    {
        var channel = await connection.CreateChannelAsync(cancellationToken: ct);
        var prefetchCount = attr.PrefetchCount >= 0 ? (ushort)attr.PrefetchCount : _options.PrefetchCount;
        await channel.BasicQosAsync(0, prefetchCount, false, ct);

        var exchangeType = attr.ExchangeType switch
        {
            RabbitMQExchangeType.Fanout => ExchangeType.Fanout,
            RabbitMQExchangeType.Topic => ExchangeType.Topic,
            RabbitMQExchangeType.Headers => ExchangeType.Headers,
            _ => ExchangeType.Direct
        };

        await channel.ExchangeDeclareAsync(attr.Exchange, exchangeType, attr.Durable, attr.AutoDelete, null, false, ct);

        var queueName = attr.Queue ?? $"{attr.Exchange}.{descriptor.HandlerType.Name}";

        // ── 队列参数：优先级、最大长度 ────────────────────────────────────
        var queueArgs = new Dictionary<string, object?>();
        if (attr.MaxPriority > 0)
            queueArgs["x-max-priority"] = (int)attr.MaxPriority;
        if (attr.MaxLength > 0)
            queueArgs["x-max-length"] = attr.MaxLength;

        channel = await DeclareQueueWithMigrationAsync(
            connection, channel, queueName, attr.Durable, attr.AutoDelete,
            queueArgs.Count > 0 ? queueArgs : null, prefetchCount, ct);

        if (attr.ExchangeType == RabbitMQExchangeType.Headers && attr.MatchHeaders is { Length: > 0 })
        {
            var bindArgs = new Dictionary<string, object?> { ["x-match"] = attr.HeaderMatchType };
            foreach (var h in attr.MatchHeaders)
            {
                var parts = h.Split('=', 2);
                if (parts.Length == 2) bindArgs[parts[0]] = parts[1];
            }
            await channel.QueueBindAsync(queueName, attr.Exchange, "", bindArgs, false, ct);
        }
        else
        {
            await channel.QueueBindAsync(queueName, attr.Exchange, attr.RoutingKey, null, false, ct);
        }

        // ── 声明重试等待队列（DLX 回到主队列） ──────────────────────────────────
        var retryWaitQueue = $"{queueName}.retry.wait";
        var retryWaitArgs = new Dictionary<string, object?>
        {
            ["x-dead-letter-exchange"] = "",
            ["x-dead-letter-routing-key"] = queueName
        };
        await channel.QueueDeclareAsync(retryWaitQueue, true, false, false, retryWaitArgs, false, ct);

        // ── 解析重试参数 ─────────────────────────────────────────────────────
        var maxRetries = attr.MaxRetries >= 0 ? attr.MaxRetries : _options.MaxRetries;
        var maxDelayRetries = attr.MaxDelayRetries >= 0 ? attr.MaxDelayRetries : _options.MaxDelayRetries;
        var retryDelayMs = (long)(attr.RetryDelaySeconds >= 0 ? attr.RetryDelaySeconds : _options.RetryDelaySeconds) * 1000;
        var enableDeadLetter = attr.EnableDeadLetter == TriState.Default ? _options.EnableDeadLetter : attr.EnableDeadLetter == TriState.Enabled;

        // ── 声明死信队列 ──────────────────────────────────────────────────────
        var deadLetterQueue = $"{queueName}.dead";
        if (enableDeadLetter)
            await channel.QueueDeclareAsync(deadLetterQueue, true, false, false, null, false, ct);

        var handleMethod = descriptor.HandlerType
            .GetMethod("HandleAsync", [descriptor.MessageType, typeof(CancellationToken)])!;
        var handlerName = descriptor.HandlerType.Name;

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            Interlocked.Increment(ref _inflightCount);
            try
            {
                await ProcessMessageAsync(channel, descriptor, handleMethod, handlerName, queueName,
                    retryWaitQueue, deadLetterQueue, maxRetries, maxDelayRetries, retryDelayMs,
                    enableDeadLetter, ea);
            }
            finally
            {
                Interlocked.Decrement(ref _inflightCount);
            }
        };

        var consumerTag = await channel.BasicConsumeAsync(queueName, false, consumer, ct);
        lock (_consumerStateLock)
        {
            _consumerRegistrations.Add(new ConsumerRegistration(descriptor, attr, channel, consumerTag));
        }

        _logger.LogInformation(
            "Consumer started: {Handler} -> {Exchange}/{RoutingKey} (retries: {MaxRetries}, delay retries: {MaxDelayRetries}/{DelaySeconds}s)",
            handlerName, attr.Exchange, attr.RoutingKey, maxRetries, maxDelayRetries, retryDelayMs / 1000);
    }

    private async Task ProcessMessageAsync(
        IChannel channel, MessageHandlerDescriptor descriptor, MethodInfo handleMethod, string handlerName,
        string queueName, string retryWaitQueue, string deadLetterQueue,
        int maxRetries, int maxDelayRetries, long retryDelayMs, bool enableDeadLetter,
        BasicDeliverEventArgs ea)
    {
        var retryCount = GetHeaderInt(ea.BasicProperties.Headers, HeaderRetryCount);
        var delayRetryCount = GetHeaderInt(ea.BasicProperties.Headers, HeaderDelayRetryCount);
        var messageId = ea.BasicProperties.MessageId ?? "unknown";

        try
        {
            var startTimestamp = Stopwatch.GetTimestamp();
            using var scope = _serviceProvider.CreateScope();
            var handler = scope.ServiceProvider.GetRequiredService(descriptor.HandlerType);
            object? message;
            try
            {
                message = JsonSerializer.Deserialize(ea.Body.Span, descriptor.MessageType);
                if (message is null)
                    throw new JsonException("Message body deserialized to null.");
            }
            catch (JsonException ex)
            {
                await HandleUnrecoverableMessageAsync(
                    channel, handlerName, queueName, deadLetterQueue,
                    enableDeadLetter, messageId, ea, ex, "Message deserialization failed");
                return;
            }

            if (!string.Equals(messageId, "unknown", StringComparison.OrdinalIgnoreCase)
                && await _deduplicator.IsDuplicateAsync(handlerName, messageId, _shutdownCts.Token))
            {
                await channel.BasicAckAsync(ea.DeliveryTag, false);
                DuplicateCounter.Add(1, new KeyValuePair<string, object?>("handler", handlerName));
                _logger.LogInformation("[{Handler}] Duplicate message detected, skipped. MessageId={MessageId}", handlerName, messageId);
                return;
            }

            await (Task)handleMethod.Invoke(handler, [message, _shutdownCts.Token])!;
            if (!string.Equals(messageId, "unknown", StringComparison.OrdinalIgnoreCase))
            {
                await _deduplicator.MarkProcessedAsync(handlerName, messageId, _shutdownCts.Token);
            }

            await channel.BasicAckAsync(ea.DeliveryTag, false);
            ProcessedCounter.Add(1, new KeyValuePair<string, object?>("handler", handlerName));
            HandleDurationMs.Record(
                Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds,
                new KeyValuePair<string, object?>("handler", handlerName));
            _logger.LogDebug("[{Handler}] Message processed successfully, MessageId={MessageId}", handlerName, messageId);
        }
        catch (Exception rawEx) when (rawEx is not OperationCanceledException)
        {
            // 解包反射调用产生的 TargetInvocationException，保留原始异常信息
            var ex = rawEx is System.Reflection.TargetInvocationException { InnerException: { } inner } ? inner : rawEx;

            if (retryCount < maxRetries)
            {
                // ── 即时重试：原地 republish ──────────────────────────────
                _logger.LogWarning(ex, "[{Handler}] Processing failed, immediate retry {Current}/{Max}, MessageId={MessageId}",
                    handlerName, retryCount + 1, maxRetries, messageId);

                var props = BuildRetryProperties(retryCount + 1, delayRetryCount, messageId);
                if (await TryRepublishAndAckOriginalAsync(
                    channel, queueName, props, ea, handlerName, messageId,
                    "Immediate retry"))
                {
                    RetryCounter.Add(1, new KeyValuePair<string, object?>("handler", handlerName));
                }
            }
            else if (delayRetryCount < maxDelayRetries)
            {
                var nextDelayRetry = delayRetryCount + 1;
                var delayMs = CalculateDelayRetryDelayMs(retryDelayMs, nextDelayRetry);

                // ── 延迟重试：投递到 retry.wait 队列 ─────────────────────
                _logger.LogWarning(ex, "[{Handler}] Immediate retries exhausted, delayed retry {Current}/{Max}, waiting {Delay}s, MessageId={MessageId}",
                    handlerName, nextDelayRetry, maxDelayRetries, delayMs / 1000, messageId);

                var props = BuildRetryProperties(0, nextDelayRetry, messageId);
                props.Expiration = delayMs.ToString();
                if (await TryRepublishAndAckOriginalAsync(
                    channel, retryWaitQueue, props, ea, handlerName, messageId,
                    "Delayed retry"))
                {
                    DelayRetryCounter.Add(1, new KeyValuePair<string, object?>("handler", handlerName));
                }
            }
            else if (enableDeadLetter)
            {
                // ── 投递到死信队列 ─────────────────────────────────────────
                _logger.LogError(ex,
                    "[{Handler}] All retries exhausted (immediate:{MaxRetries}, delayed:{MaxDelayRetries}), message moved to dead letter queue, MessageId={MessageId}",
                    handlerName, maxRetries, maxDelayRetries, messageId);

                var deadProps = new BasicProperties
                {
                    ContentType = "application/json",
                    DeliveryMode = DeliveryModes.Persistent,
                    MessageId = messageId,
                    Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
                    Headers = new Dictionary<string, object?>
                    {
                        ["x-dead-reason"] = ex.Message,
                        ["x-dead-handler"] = handlerName,
                        ["x-dead-time"] = DateTimeOffset.UtcNow.ToString("O")
                    }
                };
                if (await TryRepublishAndAckOriginalAsync(
                    channel, deadLetterQueue, deadProps, ea, handlerName, messageId,
                    "Dead letter publish"))
                {
                    DeadLetterCounter.Add(1, new KeyValuePair<string, object?>("handler", handlerName));
                }
            }
            else
            {
                // ── 死信队列未启用，直接丢弃 ───────────────────────────────
                _logger.LogError(ex,
                    "[{Handler}] All retries exhausted (immediate:{MaxRetries}, delayed:{MaxDelayRetries}), message discarded, MessageId={MessageId}",
                    handlerName, maxRetries, maxDelayRetries, messageId);
                await channel.BasicAckAsync(ea.DeliveryTag, false);
                DiscardCounter.Add(1, new KeyValuePair<string, object?>("handler", handlerName));
            }
        }
    }

    private async Task HandleUnrecoverableMessageAsync(
        IChannel channel,
        string handlerName,
        string queueName,
        string deadLetterQueue,
        bool enableDeadLetter,
        string messageId,
        BasicDeliverEventArgs ea,
        Exception ex,
        string reason)
    {
        if (!enableDeadLetter)
        {
            _logger.LogError(ex,
                "[{Handler}] {Reason}, dead letter not enabled, message discarded. Queue={Queue} MessageId={MessageId}",
                handlerName, reason, queueName, messageId);
            await channel.BasicAckAsync(ea.DeliveryTag, false);
            DiscardCounter.Add(1, new KeyValuePair<string, object?>("handler", handlerName));
            return;
        }

        var deadProps = new BasicProperties
        {
            ContentType = "application/json",
            DeliveryMode = DeliveryModes.Persistent,
            MessageId = messageId,
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            Headers = new Dictionary<string, object?>
            {
                ["x-dead-reason"] = reason,
                ["x-dead-handler"] = handlerName,
                ["x-dead-time"] = DateTimeOffset.UtcNow.ToString("O")
            }
        };

        if (await TryRepublishAndAckOriginalAsync(
                channel, deadLetterQueue, deadProps, ea, handlerName, messageId, "Unrecoverable message dead letter publish"))
        {
            DeadLetterCounter.Add(1, new KeyValuePair<string, object?>("handler", handlerName));
        }
    }

    private async Task<bool> TryRepublishAndAckOriginalAsync(
        IChannel channel,
        string destinationQueue,
        BasicProperties properties,
        BasicDeliverEventArgs ea,
        string handlerName,
        string messageId,
        string purpose)
    {
        try
        {
            await channel.BasicPublishAsync("", destinationQueue, false, properties, ea.Body, CancellationToken.None);
            await channel.BasicAckAsync(ea.DeliveryTag, false);
            return true;
        }
        catch (Exception republishEx)
        {
            _logger.LogCritical(republishEx,
                "[{Handler}] {Purpose} failed, original message remains unacknowledged pending re-delivery. MessageId={MessageId}",
                handlerName, purpose, messageId);
            return false;
        }
    }

    private long CalculateDelayRetryDelayMs(long baseDelayMs, int delayRetryAttempt)
    {
        var delayMs = baseDelayMs;
        var maxDelayMs = _options.MaxRetryDelaySeconds * 1000L;

        if (_options.EnableExponentialRetryBackoff)
        {
            var multiplier = Math.Pow(2, Math.Max(0, delayRetryAttempt - 1));
            var computed = (long)Math.Min(baseDelayMs * multiplier, long.MaxValue);
            delayMs = Math.Min(computed, maxDelayMs);
        }
        else
        {
            delayMs = Math.Min(delayMs, maxDelayMs);
        }

        if (_options.EnableRetryJitter)
        {
            var jitterUpperBound = Math.Max(1, delayMs / 5); // up to 20% jitter
            var jitter = Random.Shared.NextInt64(0, jitterUpperBound);
            delayMs = Math.Min(delayMs + jitter, maxDelayMs);
        }

        return delayMs;
    }

    private static BasicProperties BuildRetryProperties(int retryCount, int delayRetryCount, string? messageId = null)
    {
        return new BasicProperties
        {
            ContentType = "application/json",
            DeliveryMode = DeliveryModes.Persistent,
            MessageId = messageId ?? Guid.NewGuid().ToString("N"),
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            Headers = new Dictionary<string, object?>
            {
                [HeaderRetryCount] = retryCount,
                [HeaderDelayRetryCount] = delayRetryCount
            }
        };
    }

    private static int GetHeaderInt(IDictionary<string, object?>? headers, string key)
    {
        if (headers is null || !headers.TryGetValue(key, out var value)) return 0;
        return value switch
        {
            int i => i,
            long l => (int)l,
            short s => s,
            byte b => b,
            _ => 0
        };
    }

    /// <summary>
    /// 声明队列，当参数不匹配（PRECONDITION_FAILED）时自动删除旧队列并重建。
    /// PRECONDITION_FAILED 会导致当前 channel 关闭，因此需要重新创建 channel。
    /// </summary>
    private async Task<IChannel> DeclareQueueWithMigrationAsync(
        IConnection connection, IChannel channel, string queueName,
        bool durable, bool autoDelete, IDictionary<string, object?>? arguments,
        ushort prefetchCount, CancellationToken ct)
    {
        try
        {
            await channel.QueueDeclareAsync(queueName, durable, false, autoDelete, arguments, false, ct);
            return channel;
        }
        catch (OperationInterruptedException ex) when (ex.ShutdownReason?.ReplyCode == 406)
        {
            if (!_options.AutoMigrateQueues)
            {
                _logger.LogError(
                    "Queue {Queue} parameter mismatch and AutoMigrateQueues=false, manually delete queue in RabbitMQ management UI and restart. Reason: {Reason}",
                    queueName, ex.ShutdownReason.ReplyText);
                throw;
            }

            _logger.LogWarning(
                "Queue {Queue} parameter mismatch (PRECONDITION_FAILED), auto-deleting and recreating. Reason: {Reason}",
                queueName, ex.ShutdownReason.ReplyText);

            // 原 channel 已被 broker 关闭，需要新建
            channel = await connection.CreateChannelAsync(cancellationToken: ct);
            await channel.BasicQosAsync(0, prefetchCount, false, ct);
            await channel.QueueDeleteAsync(queueName, false, false, ct);
            await channel.QueueDeclareAsync(queueName, durable, false, autoDelete, arguments, false, ct);

            _logger.LogInformation("Queue {Queue} recreated successfully", queueName);
            return channel;
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("RabbitMQ consumer service shutting down, stopping new message intake...");

        List<ConsumerRegistration> registrations;
        lock (_consumerStateLock)
        {
            registrations = [.. _consumerRegistrations];
            _consumerRegistrations.Clear();
        }

        // ── 1. 取消所有消费者，停止接收新消息 ────────────────────────────
        foreach (var registration in registrations)
        {
            try
            {
                await registration.Channel.BasicCancelAsync(registration.ConsumerTag, false, cancellationToken);
            }
            catch { /* channel may already be closed */ }
        }

        // ── 2. 等待正在处理的消息完成（带超时） ──────────────────────────
        var timeout = TimeSpan.FromSeconds(_options.ShutdownTimeoutSeconds);
        var deadline = DateTime.UtcNow + timeout;
        var inflight = Volatile.Read(ref _inflightCount);

        if (inflight > 0)
        {
            _logger.LogInformation("Waiting for {Count} in-flight message(s) to complete (timeout {Timeout}s)...", inflight, _options.ShutdownTimeoutSeconds);

            while (Volatile.Read(ref _inflightCount) > 0 && DateTime.UtcNow < deadline)
            {
                await Task.Delay(200, cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }

            inflight = Volatile.Read(ref _inflightCount);
            if (inflight > 0)
                _logger.LogWarning("Shutdown timeout, {Count} message(s) still in-flight, forcing shutdown.", inflight);
            else
                _logger.LogInformation("All in-flight messages completed.");
        }

        // ── 3. 通知 Handler 中的 CancellationToken ──────────────────────
        await _shutdownCts.CancelAsync();

        // ── 4. 关闭所有 Channel ─────────────────────────────────────────
        foreach (var registration in registrations)
        {
            try
            {
                await registration.Channel.CloseAsync(cancellationToken);
                registration.Channel.Dispose();
            }
            catch { /* ignore on shutdown */ }
        }

        _logger.LogInformation("RabbitMQ consumer service stopped.");
        await base.StopAsync(cancellationToken);
    }

    public override void Dispose()
    {
        _shutdownCts.Dispose();
        base.Dispose();
    }

    private sealed record ConsumerRegistration(
        MessageHandlerDescriptor Descriptor,
        RabbitMQSubscribeAttribute Attr,
        IChannel Channel,
        string ConsumerTag);
}

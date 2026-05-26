using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace GaoXinLibrary.RabbitMQ;

/// <summary>
/// RabbitMQ 消息发布器，支持延迟队列（基于死信交换机 + 消息 TTL）、消息优先级、Publisher Confirms
/// </summary>
internal sealed class RabbitMQPublisher : IRabbitMQPublisher, IAsyncDisposable
{
    private readonly RabbitMQConnectionManager _connectionManager;
    private readonly RabbitMQOptions _options;
    private readonly ILogger<RabbitMQPublisher> _logger;
    private IChannel? _channel;
    private readonly SemaphoreSlim _channelLock = new(1, 1);
    private readonly SemaphoreSlim _publishLock = new(1, 1);
    private readonly SemaphoreSlim _delayInfraLock = new(1, 1);
    private readonly ConcurrentDictionary<string, bool> _declaredDelayInfra = new();

    private const int MaxDeclaredDelayInfraEntries = 10_000;

    public RabbitMQPublisher(
        RabbitMQConnectionManager connectionManager,
        IOptions<RabbitMQOptions> options,
        ILogger<RabbitMQPublisher> logger)
    {
        _connectionManager = connectionManager;
        _options = options.Value;
        _logger = logger;
    }

    public async Task PublishAsync<T>(
        string exchange,
        string routingKey,
        T message,
        TimeSpan? delay = null,
        IDictionary<string, object?>? headers = null,
        byte? priority = null,
        string? messageId = null,
        CancellationToken cancellationToken = default) where T : class
    {
        var body = JsonSerializer.SerializeToUtf8Bytes(message);
        var props = new BasicProperties
        {
            ContentType = "application/json",
            DeliveryMode = DeliveryModes.Persistent,
            MessageId = messageId ?? Guid.NewGuid().ToString("N"),
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds())
        };

        if (priority.HasValue)
            props.Priority = priority.Value;

        if (headers is not null)
            props.Headers = new Dictionary<string, object?>(headers);

        // RabbitMQ channel is not thread-safe. Serialize publish operations to avoid protocol errors.
        await _publishLock.WaitAsync(cancellationToken);
        try
        {
            var channel = await GetChannelAsync(cancellationToken);
            if (delay is { TotalMilliseconds: > 0 })
            {
                await EnsureDelayInfraAsync(channel, exchange, routingKey, cancellationToken);
                props.Expiration = ((long)delay.Value.TotalMilliseconds).ToString();
                try
                {
                    await channel.BasicPublishAsync(
                        $"{exchange}.delay", $"{exchange}.{routingKey}.delay",
                        false, props, body, cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Failed to publish delayed message to exchange '{Exchange}', routingKey '{RoutingKey}'",
                        $"{exchange}.delay", $"{exchange}.{routingKey}.delay");
                    throw;
                }
            }
            else
            {
                try
                {
                    await channel.BasicPublishAsync(exchange, routingKey, false, props, body, cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Failed to publish message to exchange '{Exchange}', routingKey '{RoutingKey}'",
                        exchange, routingKey);
                    throw;
                }
            }
        }
        finally
        {
            _publishLock.Release();
        }
    }

    public async Task PublishBatchAsync<T>(
        string exchange,
        string routingKey,
        IEnumerable<T> messages,
        IDictionary<string, object?>? headers = null,
        byte? priority = null,
        string? messageId = null,
        CancellationToken cancellationToken = default) where T : class
    {
        await _publishLock.WaitAsync(cancellationToken);
        try
        {
            var channel = await GetChannelAsync(cancellationToken);
            foreach (var message in messages)
            {
                var body = JsonSerializer.SerializeToUtf8Bytes(message);
                var props = new BasicProperties
                {
                    ContentType = "application/json",
                    DeliveryMode = DeliveryModes.Persistent,
                    MessageId = messageId ?? Guid.NewGuid().ToString("N"),
                    Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds())
                };

                if (priority.HasValue)
                    props.Priority = priority.Value;

                if (headers is not null)
                    props.Headers = new Dictionary<string, object?>(headers);

                try
                {
                    await channel.BasicPublishAsync(exchange, routingKey, false, props, body, cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Failed to publish batch message to exchange '{Exchange}', routingKey '{RoutingKey}'",
                        exchange, routingKey);
                    throw;
                }
            }
        }
        finally
        {
            _publishLock.Release();
        }
    }

    private async Task EnsureDelayInfraAsync(IChannel channel, string exchange, string routingKey, CancellationToken ct)
    {
        var key = $"{exchange}|{routingKey}";

        // Bounded dictionary (max 10,000 entries) to prevent unbounded growth in long-running services.
        // When full, skip the lookup — infra will be re-declared (exchange/queue declare are idempotent),
        // trading a small number of extra round-trips for bounded memory.
        if (_declaredDelayInfra.Count < MaxDeclaredDelayInfraEntries)
        {
            if (_declaredDelayInfra.ContainsKey(key))
                return;
        }

        await _delayInfraLock.WaitAsync(ct);
        try
        {
            if (_declaredDelayInfra.Count < MaxDeclaredDelayInfraEntries
                && _declaredDelayInfra.ContainsKey(key))
                return;

            var delayExchange = $"{exchange}.delay";
            var delayQueue = $"{exchange}.{routingKey}.delay";

            _logger.LogDebug("Declaring delay exchange '{DelayExchange}'", delayExchange);
            await channel.ExchangeDeclareAsync(delayExchange, ExchangeType.Direct, true, false, null, false, ct);

            var args = new Dictionary<string, object?>
            {
                ["x-dead-letter-exchange"] = exchange,
                ["x-dead-letter-routing-key"] = routingKey
            };
            _logger.LogDebug("Declaring delay queue '{DelayQueue}'", delayQueue);
            await channel.QueueDeclareAsync(delayQueue, true, false, false, args, false, ct);

            _logger.LogDebug("Binding delay queue '{DelayQueue}' to exchange '{DelayExchange}'", delayQueue, delayExchange);
            await channel.QueueBindAsync(delayQueue, delayExchange, delayQueue, null, false, ct);

            if (_declaredDelayInfra.Count < MaxDeclaredDelayInfraEntries)
                _declaredDelayInfra[key] = true;
        }
        finally
        {
            _delayInfraLock.Release();
        }
    }

    private async Task<IChannel> GetChannelAsync(CancellationToken ct)
    {
        if (_channel is { IsOpen: true }) return _channel;

        _logger.LogWarning("RabbitMQ channel is null or closed, recreating channel");

        await _channelLock.WaitAsync(ct);
        try
        {
            if (_channel is { IsOpen: true }) return _channel;
            var conn = await _connectionManager.GetConnectionAsync(ct);
            var channelOptions = new CreateChannelOptions(
                publisherConfirmationsEnabled: _options.EnablePublisherConfirms,
                publisherConfirmationTrackingEnabled: _options.EnablePublisherConfirms);
            _channel = await conn.CreateChannelAsync(channelOptions, ct);
            return _channel;
        }
        finally
        {
            _channelLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel is not null)
        {
            await _channel.CloseAsync();
        }
        _channelLock.Dispose();
        _publishLock.Dispose();
        _delayInfraLock.Dispose();
    }
}

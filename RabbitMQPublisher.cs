using System.Text.Json;
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
    private IChannel? _channel;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly HashSet<string> _declaredDelayInfra = [];

    public RabbitMQPublisher(RabbitMQConnectionManager connectionManager, IOptions<RabbitMQOptions> options)
    {
        _connectionManager = connectionManager;
        _options = options.Value;
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
        var channel = await GetChannelAsync(cancellationToken);
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

        if (delay is { TotalMilliseconds: > 0 })
        {
            await EnsureDelayInfraAsync(channel, exchange, routingKey, cancellationToken);
            props.Expiration = ((long)delay.Value.TotalMilliseconds).ToString();
            await channel.BasicPublishAsync(
                $"{exchange}.delay", $"{exchange}.{routingKey}.delay",
                false, props, body, cancellationToken);
        }
        else
        {
            await channel.BasicPublishAsync(exchange, routingKey, false, props, body, cancellationToken);
        }
    }

    private async Task EnsureDelayInfraAsync(IChannel channel, string exchange, string routingKey, CancellationToken ct)
    {
        var key = $"{exchange}|{routingKey}";
        if (!_declaredDelayInfra.Add(key)) return;

        var delayExchange = $"{exchange}.delay";
        var delayQueue = $"{exchange}.{routingKey}.delay";

        await channel.ExchangeDeclareAsync(delayExchange, ExchangeType.Direct, true, false, null, false, ct);

        var args = new Dictionary<string, object?>
        {
            ["x-dead-letter-exchange"] = exchange,
            ["x-dead-letter-routing-key"] = routingKey
        };
        await channel.QueueDeclareAsync(delayQueue, true, false, false, args, false, ct);
        await channel.QueueBindAsync(delayQueue, delayExchange, delayQueue, null, false, ct);
    }

    private async Task<IChannel> GetChannelAsync(CancellationToken ct)
    {
        if (_channel is { IsOpen: true }) return _channel;

        await _lock.WaitAsync(ct);
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
            _lock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel is not null)
        {
            await _channel.CloseAsync();
            _channel.Dispose();
        }
        _lock.Dispose();
    }
}

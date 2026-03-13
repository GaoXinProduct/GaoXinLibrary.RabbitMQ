using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace GaoXinLibrary.RabbitMQ;

/// <summary>
/// 管理 RabbitMQ 连接生命周期，单例复用，支持自动重连
/// </summary>
internal sealed class RabbitMQConnectionManager : IAsyncDisposable
{
    private readonly RabbitMQOptions _options;
    private readonly ILogger<RabbitMQConnectionManager> _logger;
    private IConnection? _connection;
    private readonly SemaphoreSlim _lock = new(1, 1);

    public RabbitMQConnectionManager(IOptions<RabbitMQOptions> options, ILogger<RabbitMQConnectionManager> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task<IConnection> GetConnectionAsync(CancellationToken ct = default)
    {
        if (_connection is { IsOpen: true }) return _connection;

        await _lock.WaitAsync(ct);
        try
        {
            if (_connection is { IsOpen: true }) return _connection;

            var factory = new ConnectionFactory
            {
                HostName = _options.HostName,
                Port = _options.Port,
                UserName = _options.UserName,
                Password = _options.Password,
                VirtualHost = _options.VirtualHost,
                AutomaticRecoveryEnabled = _options.AutomaticRecoveryEnabled,
                NetworkRecoveryInterval = _options.NetworkRecoveryInterval
            };

            _connection = await CreateConnectionWithRetryAsync(factory, ct);
            return _connection;
        }
        finally
        {
            _lock.Release();
        }
    }

    private async Task<IConnection> CreateConnectionWithRetryAsync(ConnectionFactory factory, CancellationToken ct)
    {
        var maxRetries = _options.ReconnectMaxRetries;
        var delay = _options.ReconnectInitialDelay;
        var attempt = 0;

        while (true)
        {
            try
            {
                var conn = await factory.CreateConnectionAsync(ct);
                if (attempt > 0)
                    _logger.LogInformation("RabbitMQ 连接已恢复（第 {Attempt} 次重试）", attempt);
                return conn;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                attempt++;
                if (maxRetries > 0 && attempt >= maxRetries)
                {
                    _logger.LogCritical(ex, "RabbitMQ 连接失败，已达最大重试次数 {Max}，放弃连接", maxRetries);
                    throw;
                }

                _logger.LogWarning(ex, "RabbitMQ 连接失败（第 {Attempt} 次），将在 {Delay}s 后重试",
                    attempt, delay.TotalSeconds);

                await Task.Delay(delay, ct);
                delay = TimeSpan.FromMilliseconds(
                    Math.Min(delay.TotalMilliseconds * 2, _options.ReconnectMaxDelay.TotalMilliseconds));
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
        {
            await _connection.CloseAsync();
            _connection.Dispose();
        }
        _lock.Dispose();
    }
}

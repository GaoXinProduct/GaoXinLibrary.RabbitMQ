using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace GaoXinLibrary.RabbitMQ;

/// <summary>
/// 管理 RabbitMQ 连接生命周期，单例复用
/// </summary>
internal sealed class RabbitMQConnectionManager : IAsyncDisposable
{
    private readonly RabbitMQOptions _options;
    private IConnection? _connection;
    private readonly SemaphoreSlim _lock = new(1, 1);

    public RabbitMQConnectionManager(IOptions<RabbitMQOptions> options)
    {
        _options = options.Value;
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
                VirtualHost = _options.VirtualHost
            };
            _connection = await factory.CreateConnectionAsync(ct);
            return _connection;
        }
        finally
        {
            _lock.Release();
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

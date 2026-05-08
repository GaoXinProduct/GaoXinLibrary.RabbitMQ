using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace GaoXinLibrary.RabbitMQ;

/// <summary>
/// RabbitMQ 健康检查，验证连接是否可用
/// </summary>
internal sealed class RabbitMQHealthCheck : IHealthCheck
{
    private readonly RabbitMQConnectionManager _connectionManager;
    private readonly RabbitMQOptions _options;

    public RabbitMQHealthCheck(RabbitMQConnectionManager connectionManager, IOptions<RabbitMQOptions> options)
    {
        _connectionManager = connectionManager;
        _options = options.Value;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(TimeSpan.FromSeconds(_options.HealthCheckTimeoutSeconds));
            var connection = await _connectionManager.GetConnectionAsync(timeoutCts.Token);
            if (connection.IsOpen)
            {
                return HealthCheckResult.Healthy("RabbitMQ 连接正常");
            }

            return HealthCheckResult.Unhealthy("RabbitMQ 连接已断开");
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return HealthCheckResult.Unhealthy($"RabbitMQ 健康检查超时（>{_options.HealthCheckTimeoutSeconds}s）");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("RabbitMQ 连接失败", ex);
        }
    }
}

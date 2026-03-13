using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace GaoXinLibrary.RabbitMQ;

/// <summary>
/// RabbitMQ 健康检查，验证连接是否可用
/// </summary>
internal sealed class RabbitMQHealthCheck : IHealthCheck
{
    private readonly RabbitMQConnectionManager _connectionManager;

    public RabbitMQHealthCheck(RabbitMQConnectionManager connectionManager)
    {
        _connectionManager = connectionManager;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            var connection = await _connectionManager.GetConnectionAsync(cancellationToken);
            if (connection.IsOpen)
            {
                return HealthCheckResult.Healthy("RabbitMQ 连接正常");
            }

            return HealthCheckResult.Unhealthy("RabbitMQ 连接已断开");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("RabbitMQ 连接失败", ex);
        }
    }
}

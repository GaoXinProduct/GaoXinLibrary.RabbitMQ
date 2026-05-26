using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace GaoXinLibrary.RabbitMQ;

/// <summary>
/// RabbitMQ health check that verifies connection availability
/// </summary>
internal sealed class RabbitMQHealthCheck : IHealthCheck
{
    private readonly RabbitMQConnectionManager _connectionManager;
    private readonly RabbitMQOptions _options;
    private readonly ILogger<RabbitMQHealthCheck> _logger;

    public RabbitMQHealthCheck(
        RabbitMQConnectionManager connectionManager,
        IOptions<RabbitMQOptions> options,
        ILogger<RabbitMQHealthCheck> logger)
    {
        _connectionManager = connectionManager;
        _options = options.Value;
        _logger = logger;
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
                _logger.LogInformation("RabbitMQ health check passed, connection is open");
                return HealthCheckResult.Healthy("RabbitMQ connection is healthy");
            }

            _logger.LogWarning("RabbitMQ health check failed, connection is closed");
            return HealthCheckResult.Unhealthy("RabbitMQ connection is closed");
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning("RabbitMQ health check timed out after {TimeoutSeconds}s", _options.HealthCheckTimeoutSeconds);
            return HealthCheckResult.Unhealthy($"RabbitMQ health check timed out (>{_options.HealthCheckTimeoutSeconds}s)");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "RabbitMQ health check failed with exception");
            return HealthCheckResult.Unhealthy("RabbitMQ connection failed", ex);
        }
    }
}

using System.ComponentModel.DataAnnotations;

namespace GaoXinLibrary.RabbitMQ;

/// <summary>
/// RabbitMQ 连接配置
/// </summary>
public class RabbitMQOptions
{
    /// <summary>主机名</summary>
    [Required(ErrorMessage = "RabbitMQ HostName 不能为空")]
    public string HostName { get; set; } = "localhost";

    /// <summary>端口</summary>
    [Range(1, 65535, ErrorMessage = "RabbitMQ Port 必须在 1-65535 范围内")]
    public int Port { get; set; } = 5672;

    /// <summary>用户名</summary>
    [Required(ErrorMessage = "RabbitMQ UserName 不能为空")]
    public string UserName { get; set; } = "guest";

    /// <summary>密码</summary>
    [Required(ErrorMessage = "RabbitMQ Password 不能为空")]
    public string Password { get; set; } = "guest";

    /// <summary>虚拟主机</summary>
    [Required(ErrorMessage = "RabbitMQ VirtualHost 不能为空")]
    public string VirtualHost { get; set; } = "/";

    /// <summary>每个消费者预取消息数</summary>
    [Range(1, 65535, ErrorMessage = "PrefetchCount 必须大于 0")]
    public ushort PrefetchCount { get; set; } = 10;

    /// <summary>即时重试最大次数（失败后立即重试，默认 3 次）</summary>
    [Range(0, int.MaxValue, ErrorMessage = "MaxRetries 不能为负数")]
    public int MaxRetries { get; set; } = 3;

    /// <summary>延迟重试最大次数（即时重试耗尽后进入延迟队列，默认 3 次）</summary>
    [Range(0, int.MaxValue, ErrorMessage = "MaxDelayRetries 不能为负数")]
    public int MaxDelayRetries { get; set; } = 3;

    /// <summary>延迟重试间隔秒数（默认 10 秒）</summary>
    [Range(1, int.MaxValue, ErrorMessage = "RetryDelaySeconds 必须大于 0")]
    public int RetryDelaySeconds { get; set; } = 10;

    /// <summary>是否对延迟重试启用指数退避（默认 false，保持兼容固定延迟）</summary>
    public bool EnableExponentialRetryBackoff { get; set; }

    /// <summary>重试最大延迟秒数（用于指数退避上限，默认 300 秒）</summary>
    [Range(1, int.MaxValue, ErrorMessage = "MaxRetryDelaySeconds 必须大于 0")]
    public int MaxRetryDelaySeconds { get; set; } = 300;

    /// <summary>是否在延迟重试中加入抖动（默认 true）</summary>
    public bool EnableRetryJitter { get; set; } = true;

    /// <summary>是否启用死信队列（默认 true，重试耗尽后消息投递到 {queue}.dead）</summary>
    public bool EnableDeadLetter { get; set; } = true;

    /// <summary>是否启用 Publisher Confirms（默认 false）</summary>
    public bool EnablePublisherConfirms { get; set; }

    /// <summary>队列参数不匹配时是否自动删除并重建队列（默认 false，避免生产误删）</summary>
    public bool AutoMigrateQueues { get; set; }

    /// <summary>优雅关闭超时秒数（等待正在处理的消息完成，默认 30 秒）</summary>
    [Range(1, int.MaxValue, ErrorMessage = "ShutdownTimeoutSeconds 必须大于 0")]
    public int ShutdownTimeoutSeconds { get; set; } = 30;

    /// <summary>健康检查连接超时秒数（默认 2 秒）</summary>
    [Range(1, 60, ErrorMessage = "HealthCheckTimeoutSeconds 必须在 1-60 范围内")]
    public int HealthCheckTimeoutSeconds { get; set; } = 2;

    /// <summary>是否启用 RabbitMQ.Client 内置的自动恢复（默认 true）</summary>
    public bool AutomaticRecoveryEnabled { get; set; } = true;

    /// <summary>自动恢复间隔（默认 5 秒）</summary>
    public TimeSpan NetworkRecoveryInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>连接断开后重连最大重试次数（默认 10，设为 0 则不限次数一直重试）</summary>
    [Range(0, int.MaxValue, ErrorMessage = "ReconnectMaxRetries 不能为负数")]
    public int ReconnectMaxRetries { get; set; } = 10;

    /// <summary>重连初始延迟（默认 1 秒，指数退避）</summary>
    public TimeSpan ReconnectInitialDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>重连最大延迟（默认 30 秒）</summary>
    public TimeSpan ReconnectMaxDelay { get; set; } = TimeSpan.FromSeconds(30);
}

namespace GaoXinLibrary.RabbitMQ;

/// <summary>
/// 标记消息处理器的订阅信息，用于自动绑定交换机和队列
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
public sealed class RabbitMQSubscribeAttribute : Attribute
{
    /// <summary>交换机名称</summary>
    public string Exchange { get; }

    /// <summary>路由键（Fanout 交换机可忽略）</summary>
    public string RoutingKey { get; }

    /// <summary>交换机类型</summary>
    public RabbitMQExchangeType ExchangeType { get; }

    /// <summary>自定义队列名称，为空则自动生成</summary>
    public string? Queue { get; set; }

    /// <summary>是否持久化（默认 true）</summary>
    public bool Durable { get; set; } = true;

    /// <summary>是否自动删除（默认 false）</summary>
    public bool AutoDelete { get; set; }

    /// <summary>Headers 交换机匹配模式：all 或 any</summary>
    public string HeaderMatchType { get; set; } = "all";

    /// <summary>Headers 交换机匹配参数，格式："key=value"</summary>
    public string[]? MatchHeaders { get; set; }

    /// <summary>即时重试最大次数（-1 使用全局配置）</summary>
    public int MaxRetries { get; set; } = -1;

    /// <summary>延迟重试最大次数（-1 使用全局配置）</summary>
    public int MaxDelayRetries { get; set; } = -1;

    /// <summary>延迟重试间隔秒数（-1 使用全局配置）</summary>
    public int RetryDelaySeconds { get; set; } = -1;

    /// <summary>队列最大优先级（0 表示不启用优先级，建议 1-10）</summary>
    public byte MaxPriority { get; set; }

    /// <summary>队列最大长度（0 表示不限制）</summary>
    public int MaxLength { get; set; }

    /// <summary>是否启用死信队列（-1 使用全局配置，0 禁用，1 启用）</summary>
    public int EnableDeadLetter { get; set; } = -1;

    /// <summary>消费者预取数（-1 使用全局配置，优先级队列建议设为 1）</summary>
    public int PrefetchCount { get; set; } = -1;

    public RabbitMQSubscribeAttribute(string exchange, string routingKey = "", RabbitMQExchangeType exchangeType = RabbitMQExchangeType.Fanout)
    {
        Exchange = exchange;
        RoutingKey = routingKey;
        ExchangeType = exchangeType;
    }
}

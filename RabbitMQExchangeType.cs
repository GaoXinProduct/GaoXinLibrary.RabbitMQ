namespace GaoXinLibrary.RabbitMQ;

/// <summary>
/// RabbitMQ 交换机类型
/// </summary>
public enum RabbitMQExchangeType
{
    /// <summary>直连交换机</summary>
    Direct,

    /// <summary>扇出交换机（广播）</summary>
    Fanout,

    /// <summary>主题交换机（通配符路由）</summary>
    Topic,

    /// <summary>头部交换机（基于消息头匹配）</summary>
    Headers
}

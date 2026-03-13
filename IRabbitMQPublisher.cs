namespace GaoXinLibrary.RabbitMQ;

/// <summary>
/// RabbitMQ 消息发布器接口
/// </summary>
public interface IRabbitMQPublisher
{
    /// <summary>
    /// 发布消息到指定交换机
    /// </summary>
    /// <param name="exchange">交换机名称</param>
    /// <param name="routingKey">路由键</param>
    /// <param name="message">消息对象（将序列化为 JSON）</param>
    /// <param name="delay">延迟投递时间，为空则立即投递</param>
    /// <param name="headers">自定义消息头</param>
    /// <param name="priority">消息优先级（0-255，需队列声明 x-max-priority），为空则不设置</param>
    /// <param name="messageId">消息唯一标识，用于全链路追踪，为空则自动生成 GUID</param>
    /// <param name="cancellationToken">取消令牌</param>
    Task PublishAsync<T>(
        string exchange,
        string routingKey,
        T message,
        TimeSpan? delay = null,
        IDictionary<string, object?>? headers = null,
        byte? priority = null,
        string? messageId = null,
        CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// 批量发布消息到指定交换机，性能优于逐条发布
    /// </summary>
    /// <param name="exchange">交换机名称</param>
    /// <param name="routingKey">路由键</param>
    /// <param name="messages">消息集合</param>
    /// <param name="headers">自定义消息头（所有消息共用）</param>
    /// <param name="priority">消息优先级（所有消息共用）</param>
    /// <param name="cancellationToken">取消令牌</param>
    Task PublishBatchAsync<T>(
        string exchange,
        string routingKey,
        IEnumerable<T> messages,
        IDictionary<string, object?>? headers = null,
        byte? priority = null,
        CancellationToken cancellationToken = default) where T : class;
}

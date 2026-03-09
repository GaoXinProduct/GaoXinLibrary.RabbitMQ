namespace GaoXinLibrary.RabbitMQ;

/// <summary>
/// 消息处理器接口，实现此接口并配合 <see cref="RabbitMQSubscribeAttribute"/> 即可自动订阅消费
/// </summary>
/// <typeparam name="TMessage">消息类型</typeparam>
public interface IMessageHandler<in TMessage> where TMessage : class
{
    Task HandleAsync(TMessage message, CancellationToken cancellationToken = default);
}

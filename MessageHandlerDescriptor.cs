namespace GaoXinLibrary.RabbitMQ;

/// <summary>
/// 消息处理器描述符，用于运行时发现和调用处理器
/// </summary>
internal sealed class MessageHandlerDescriptor
{
    public Type HandlerType { get; }
    public Type MessageType { get; }

    public MessageHandlerDescriptor(Type handlerType, Type messageType)
    {
        HandlerType = handlerType;
        MessageType = messageType;
    }
}

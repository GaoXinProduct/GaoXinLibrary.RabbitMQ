namespace GaoXinLibrary.RabbitMQ;

/// <summary>
/// 消息幂等扩展点：可由业务侧实现去重策略（Redis/数据库等），默认实现为 No-Op。
/// </summary>
public interface IMessageDeduplicator
{
    /// <summary>
    /// 判断该消息是否已被处理。返回 true 表示应跳过处理。
    /// </summary>
    Task<bool> IsDuplicateAsync(string handlerName, string messageId, CancellationToken cancellationToken = default);

    /// <summary>
    /// 标记消息处理完成。
    /// </summary>
    Task MarkProcessedAsync(string handlerName, string messageId, CancellationToken cancellationToken = default);
}

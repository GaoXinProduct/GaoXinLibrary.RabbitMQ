namespace GaoXinLibrary.RabbitMQ;

/// <summary>
/// 默认幂等实现：不做去重，保持现有行为兼容。
/// </summary>
internal sealed class NoOpMessageDeduplicator : IMessageDeduplicator
{
    public Task<bool> IsDuplicateAsync(string handlerName, string messageId, CancellationToken cancellationToken = default)
        => Task.FromResult(false);

    public Task MarkProcessedAsync(string handlerName, string messageId, CancellationToken cancellationToken = default)
        => Task.CompletedTask;
}

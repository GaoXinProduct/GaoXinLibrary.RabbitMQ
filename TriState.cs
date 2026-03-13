namespace GaoXinLibrary.RabbitMQ;

/// <summary>
/// 三态开关，用于属性中需要区分"未设置/启用/禁用"的场景
/// </summary>
public enum TriState
{
    /// <summary>使用全局配置</summary>
    Default = 0,

    /// <summary>启用</summary>
    Enabled = 1,

    /// <summary>禁用</summary>
    Disabled = 2
}

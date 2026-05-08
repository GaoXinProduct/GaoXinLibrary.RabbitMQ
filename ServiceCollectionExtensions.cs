using System.ComponentModel.DataAnnotations;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace GaoXinLibrary.RabbitMQ;

/// <summary>
/// DI 注入扩展方法
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// 注册 RabbitMQ 服务（连接管理、发布器、消费者托管服务）
    /// </summary>
    public static IServiceCollection AddRabbitMQ(this IServiceCollection services, Action<RabbitMQOptions> configure)
    {
        var options = new RabbitMQOptions();
        configure(options);
        Validator.ValidateObject(options, new ValidationContext(options), validateAllProperties: true);

        services.Configure(configure);
        services.AddSingleton<RabbitMQConnectionManager>();
        services.TryAddSingleton<IMessageDeduplicator, NoOpMessageDeduplicator>();
        services.AddSingleton<IRabbitMQPublisher, RabbitMQPublisher>();
        services.AddHostedService<RabbitMQConsumerHostedService>();
        return services;
    }

    /// <summary>
    /// 注册 RabbitMQ 健康检查，可通过 ASP.NET Core 的 /health 端点检测 RabbitMQ 连接状态
    /// </summary>
    /// <param name="services">服务集合</param>
    /// <param name="name">健康检查名称，默认 "rabbitmq"</param>
    /// <param name="tags">健康检查标签</param>
    public static IServiceCollection AddRabbitMQHealthCheck(
        this IServiceCollection services, string name = "rabbitmq", params string[] tags)
    {
        services.AddHealthChecks()
            .Add(new Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckRegistration(
                name,
                sp => sp.GetRequiredService<RabbitMQHealthCheck>(),
                failureStatus: Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus.Unhealthy,
                tags));
        services.AddSingleton<RabbitMQHealthCheck>();
        return services;
    }

    /// <summary>
    /// 注册单个消息处理器
    /// </summary>
    public static IServiceCollection AddRabbitMQHandler<THandler>(this IServiceCollection services) where THandler : class
    {
        return services.RegisterHandler(typeof(THandler));
    }

    /// <summary>
    /// 自动扫描指定程序集中所有标注了 <see cref="RabbitMQSubscribeAttribute"/> 的 <see cref="IMessageHandler{TMessage}"/> 实现并注册
    /// </summary>
    /// <param name="services">服务集合</param>
    /// <param name="assemblies">要扫描的程序集，为空则扫描调用方所在程序集</param>
    public static IServiceCollection AddRabbitMQHandlers(this IServiceCollection services, params Assembly[] assemblies)
    {
        if (assemblies.Length == 0)
            assemblies = [Assembly.GetEntryAssembly() ?? Assembly.GetCallingAssembly()];

        foreach (var assembly in assemblies)
        {
            var handlerTypes = assembly.GetTypes()
                .Where(t => t is { IsClass: true, IsAbstract: false }
                    && t.GetCustomAttribute<RabbitMQSubscribeAttribute>() is not null
                    && t.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IMessageHandler<>)));

            foreach (var handlerType in handlerTypes)
                services.RegisterHandler(handlerType);
        }

        return services;
    }

    private static IServiceCollection RegisterHandler(this IServiceCollection services, Type handlerType)
    {
        var handlerInterface = handlerType.GetInterfaces()
            .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IMessageHandler<>))
            ?? throw new ArgumentException($"{handlerType.Name} 必须实现 IMessageHandler<TMessage> 接口。");

        var messageType = handlerInterface.GetGenericArguments()[0];
        services.AddScoped(handlerType);
        services.AddSingleton(new MessageHandlerDescriptor(handlerType, messageType));
        return services;
    }
}

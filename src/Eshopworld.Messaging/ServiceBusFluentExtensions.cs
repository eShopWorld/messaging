namespace Eshopworld.Messaging
{
    using System;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.Management.ServiceBus.Fluent;

    /// <summary>
    /// Contains extensions to the ServiceBus Fluent SDK: <see cref="Microsoft.Azure.Management.ServiceBus.Fluent"/>.
    /// </summary>
    public static class ServiceBusNamespaceExtensions
    {
        /// <summary>
        /// Creates a specific queue if it doesn't exist in the target namespace.
        /// </summary>
        /// <param name="sbNamespace">The <see cref="IServiceBusNamespace"/> where we are creating the queue in.</param>
        /// <param name="name">The name of the queue that we are looking for.</param>
        /// <returns>The <see cref="IQueue"/> entity object that references the Azure queue.</returns>
        public static async Task<IQueue> CreateQueueIfNotExists(this IServiceBusNamespace sbNamespace, string name)
        {
            await sbNamespace.RefreshAsync();

            var queue = (await sbNamespace.Queues.ListAsync()).SingleOrDefault(q => q.Name == name.ToLower());
            if (queue != null) return queue;

            await sbNamespace.Queues
                             .Define(name.ToLower())
                             .WithMessageLockDurationInSeconds(60)
                             .WithDuplicateMessageDetection(TimeSpan.FromMinutes(10))
                             .WithExpiredMessageMovedToDeadLetterQueue()
                             .WithMessageMovedToDeadLetterQueueOnMaxDeliveryCount(10)
                             .CreateAsync();

            await sbNamespace.RefreshAsync();
            return (await sbNamespace.Queues.ListAsync()).Single(q => q.Name == name.ToLower());
        }

        /// <summary>
        /// Creates a specific topic if it doesn't exist in the target namespace.
        /// </summary>
        /// <param name="sbNamespace">The <see cref="IServiceBusNamespace"/> where we are creating the topic in.</param>
        /// <param name="name">The name of the topic that we are looking for.</param>
        /// <returns>The <see cref="ITopic"/> entity object that references the Azure topic.</returns>
        public static async Task<ITopic> CreateTopicIfNotExists(this IServiceBusNamespace sbNamespace, string name)
        {
            await sbNamespace.RefreshAsync();

            var topic = (await sbNamespace.Topics.ListAsync()).SingleOrDefault(t => t.Name == name.ToLower());
            if (topic != null) return topic;

            await sbNamespace.Topics
                .Define(name.ToLower())
                .WithDuplicateMessageDetection(TimeSpan.FromMinutes(10))
                .CreateAsync();

            await sbNamespace.RefreshAsync();
            return (await sbNamespace.Topics.ListAsync()).Single(t => t.Name == name.ToLower());
        }

        /// <summary>
        /// Creates a specific subscription to a topic if it doesn't exist yet.
        /// </summary>
        /// <param name="topic">The <see cref="ITopic"/> that we are subscribing to.</param>
        /// <param name="name">The name of the subscription we are doing on the <see cref="ITopic"/>.</param>
        /// <returns>The <see cref="ISubscription"/> entity object that references the subscription.</returns>
        public static async Task<ISubscription> CreateSubscriptionIfNotExists(this ITopic topic, string name)
        {
            await topic.RefreshAsync();

            var subscription = (await topic.Subscriptions.ListAsync()).SingleOrDefault(s => s.Name == name.ToLower());
            if (subscription != null) return subscription;

            await topic.Subscriptions
                .Define(name.ToLower())
                .WithMessageLockDurationInSeconds(60)
                .WithExpiredMessageMovedToDeadLetterSubscription()
                .WithMessageMovedToDeadLetterSubscriptionOnMaxDeliveryCount(10)
                .CreateAsync();

            await topic.RefreshAsync();
            return (await topic.Subscriptions.ListAsync()).Single(t => t.Name == name.ToLower());
        }
    }

    /// <summary>
    /// Contains extensions methods that are <see cref="IServiceBusNamespace"/> related but extend
    /// the system <see cref="Type"/> instead.
    /// </summary>
    public static class TypeExtensions
    {
        /// <summary>
        /// Gets a queue/topic name off a system <see cref="Type"/> by using the <see cref="Type"/>.FullName.
        /// If you're in DEBUG with the debugger attached, then the full name is appended by a '-' followed by
        /// an Environment.Username, giving you a named queue during debug cycles to avoid queue name clashes.
        /// </summary>
        /// <param name="type">The message <see cref="Type"/> that this queue/topic is for.</param>
        /// <returns>The final name of the queue/topic.</returns>
        public static string GetEntityName(this Type type)
        {
#if DEBUG
            var queueName = type.Name;
            if (Debugger.IsAttached)
            {
                queueName += $"-{Environment.UserName.Replace("$", "")}";
            }

            return queueName?.ToLower();
#else
            return type.Name?.ToLower();
#endif
        }
    }
}

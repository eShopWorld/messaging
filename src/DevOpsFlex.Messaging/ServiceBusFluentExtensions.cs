namespace DevOpsFlex.Messaging
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
        /// <param name="sbNamespace">The <see cref="IServiceBusNamespace"/> client that we are using to query entities inside the namespace.</param>
        /// <param name="name">The name of the queue that we are looking for.</param>
        /// <returns>The <see cref="IQueue"/> entity object that references the real queue.</returns>
        public static async Task<IQueue> CreateQueueIfNotExists(this IServiceBusNamespace sbNamespace, string name)
        {
            var queue = (await sbNamespace.Queues.ListAsync()).SingleOrDefault(q => q.Name == name);
            if (queue != null) return queue;

            await sbNamespace.Queues
                             .Define(name)
                             .WithMessageLockDurationInSeconds(60)
                             .WithDuplicateMessageDetection(TimeSpan.FromMinutes(10))
                             .WithExpiredMessageMovedToDeadLetterQueue()
                             .WithMessageMovedToDeadLetterQueueOnMaxDeliveryCount(10)
                             .CreateAsync();

            await sbNamespace.RefreshAsync();
            return (await sbNamespace.Queues.ListAsync()).Single(q => q.Name == name.ToLower());
        }
    }

    /// <summary>
    /// Contains extensions methods that are <see cref="IServiceBusNamespace"/> related but extend
    /// the system <see cref="Type"/> instead.
    /// </summary>
    public static class TypeExtensions
    {
        /// <summary>
        /// Gets a queue name off a system <see cref="Type"/> by using the <see cref="Type"/>.FullName.
        /// If you're in DEBUG with the debugger attached, then the full name is appended by a '-' followed by
        /// an Environment.Username, giving you a named queue during debug cycles to avoid queue name clashes.
        /// </summary>
        /// <param name="type">The message <see cref="Type"/> that this queue is for.</param>
        /// <returns>The final name of the queue.</returns>
        public static string GetQueueName(this Type type)
        {
#if DEBUG
            var queueName = type.FullName;
            if (Debugger.IsAttached)
            {
                queueName += $"-{Environment.UserName.Replace("$", "")}";
            }

            return queueName?.ToLower();
#else
            return type.FullName?.ToLower();
#endif
        }
    }
}

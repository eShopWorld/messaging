namespace DevOpsFlex.Messaging
{
    using System;
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

            return (await sbNamespace.Queues.ListAsync()).Single(q => q.Name == name);
        }
    }
}

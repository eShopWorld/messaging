namespace Eshopworld.Messaging.Tests
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using FluentAssertions;
    using Microsoft.Azure.Management.ServiceBus.Fluent;

    /// <summary>
    /// Contains extension methods for <see cref="Microsoft.Azure.Management.Fluent"/> around ServiceBus.
    /// </summary>
    public static class ServiceBusFluentExtensions
    {
        /// <summary>
        /// Scorches the entire service bus namespace.
        /// Currently this wipes out all queues and topics. This is used mostly by integration tests, to guarantee that
        /// both queue and topic creation processes are in place and working as intended.
        /// </summary>
        /// <param name="sbNamespace">The <see cref="IServiceBusNamespace"/> that we are scorching.</param>
        /// <returns>The async <see cref="Task"/> wrapper.</returns>
        public static async Task ScorchNamespace(this IServiceBusNamespace sbNamespace)
        {
            foreach (var queue in await sbNamespace.Queues.ListAsync())
            {
                await sbNamespace.Queues.DeleteByNameAsync(queue.Name);
            }

            foreach (var topic in await sbNamespace.Topics.ListAsync())
            {
                await sbNamespace.Topics.DeleteByNameAsync(topic.Name);
            }
        }

        public static void AssertSingleQueueExists(this IServiceBusNamespace sbNamespace, Type type)
        {
            sbNamespace.Refresh();
            var queues = sbNamespace.Queues.List().ToList();

            queues.Count.Should().Be(1);
            queues.SingleOrDefault(q => string.Equals(q.Name, type.GetQueueName(), StringComparison.CurrentCultureIgnoreCase)).Should().NotBeNull();
        }
    }
}

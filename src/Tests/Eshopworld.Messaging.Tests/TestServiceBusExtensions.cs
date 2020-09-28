using System;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Azure.Management.ServiceBus.Fluent;

namespace Eshopworld.Messaging.Tests
{
    /// <summary>
    /// Contains extension methods for <see cref="Microsoft.Azure.Management.Fluent"/> around ServiceBus.
    /// </summary>
    public static class TestServiceBusExtensions
    {
        /// <summary>
        /// Scorches the entire service bus namespace.
        /// Currently this wipes out all queues and topics. This is used mostly by integration tests, to guarantee that
        /// both queue and topic creation processes are in place and working as intended.
        /// </summary>
        /// <param name="sbNamespace">The <see cref="IServiceBusNamespace"/> that we are scorching.</param>
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

        /// <summary>
        /// Checks if a given queue exists to facilitate tests that scorch the namespace and check if the queue was properly created.
        /// </summary>
        /// <param name="sbNamespace">The <see cref="IServiceBusNamespace"/> that we are checking in.</param>
        /// <param name="type">The message <see cref="Type"/> that we are checking the queue for.</param>
        public static void AssertSingleQueueExists(this IServiceBusNamespace sbNamespace, Type type)
        {
            sbNamespace.Refresh();
            var queues = sbNamespace.Queues.List().ToList();

            queues.Count.Should().Be(1);
            queues.SingleOrDefault(q => string.Equals(q.Name, type.GetEntityName(), StringComparison.CurrentCultureIgnoreCase)).Should().NotBeNull();
        }

        /// <summary>
        /// Checks if a given topic exists to facilitate tests that scorch the namespace and check if the topic was properly created.
        /// </summary>
        /// <param name="sbNamespace">The <see cref="IServiceBusNamespace"/> that we are checking in.</param>
        /// <param name="type">The event <see cref="Type"/> that we are checking the topic for.</param>
        public static void AssertSingleTopicExists(this IServiceBusNamespace sbNamespace, Type type)
        {
            AssertSingleTopicExists(sbNamespace, type.GetEntityName());
        }

        /// <summary>
        /// Checks if a given topic exists to facilitate tests that scorch the namespace and check if the topic was properly created.
        /// </summary>
        /// <param name="sbNamespace">The <see cref="IServiceBusNamespace"/> that we are checking in.</param>
        /// <param name="topicName">The topic name</param>
        public static void AssertSingleTopicExists(this IServiceBusNamespace sbNamespace, string topicName)
        {
            sbNamespace.Refresh();
            var topics = sbNamespace.Topics.List().ToList();

            topics.Count.Should().Be(1);
            topics.SingleOrDefault(s => string.Equals(s.Name, topicName, StringComparison.CurrentCultureIgnoreCase)).Should().NotBeNull();
        }


        /// <summary>
        /// Checks if a given topic exists to facilitate tests that scorch the namespace and check if the topic was properly created.
        /// </summary>
        /// <param name="sbNamespace">The <see cref="IServiceBusNamespace"/> that we are checking in.</param>
        /// <param name="type">The event <see cref="Type"/> that we are checking the topic for.</param>
        /// <param name="subscriptionName">The name of the subscription that we are checking on the topic.</param>
        public static void AssertSingleTopicSubscriptionExists(this IServiceBusNamespace sbNamespace, Type type, string subscriptionName)
        {
            AssertSingleTopicSubscriptionExists(sbNamespace, type.GetEntityName(),subscriptionName);
        }

        /// <summary>
        /// Checks if a given topic exists to facilitate tests that scorch the namespace and check if the topic was properly created.
        /// </summary>
        /// <param name="sbNamespace">The <see cref="IServiceBusNamespace"/> that we are checking in.</param>
        /// <param name="topicName">The topic name we are checking for</param>
        /// <param name="subscriptionName">The name of the subscription that we are checking on the topic.</param>
        public static void AssertSingleTopicSubscriptionExists(this IServiceBusNamespace sbNamespace, string topicName, string subscriptionName)
        {
            sbNamespace.Refresh();
            var subscriptions = sbNamespace.Topics.GetByName(topicName).Subscriptions.List().ToList();

            subscriptions.Count.Should().Be(1);
            subscriptions.SingleOrDefault(t => string.Equals(t.Name, subscriptionName, StringComparison.CurrentCultureIgnoreCase)).Should().NotBeNull();
        }
    }
}

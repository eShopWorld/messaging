using System;
using System.Diagnostics;
using System.Net;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Eshopworld.Core;
using Microsoft.Azure.Management.ServiceBus.Fluent;

namespace Eshopworld.Messaging
{
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
            var queue = await sbNamespace.GetQueueByName(name); 
            if (queue != null) return queue;

            queue = await sbNamespace.Queues
                             .Define(name.ToLower())
                             .WithMessageLockDurationInSeconds(60)
                             .WithDuplicateMessageDetection(TimeSpan.FromMinutes(10))
                             .WithExpiredMessageMovedToDeadLetterQueue()
                             .WithMessageMovedToDeadLetterQueueOnMaxDeliveryCount(10)
                             .CreateAsync();

            await sbNamespace.RefreshAsync();
            return queue;
        }

        /// <summary>
        /// Creates a specific topic if it doesn't exist in the target namespace.
        /// </summary>
        /// <param name="sbNamespace">The <see cref="IServiceBusNamespace"/> where we are creating the topic in.</param>
        /// <param name="name">The name of the topic that we are looking for.</param>
        /// <returns>The <see cref="ITopic"/> entity object that references the Azure topic.</returns>
        public static async Task<ITopic> CreateTopicIfNotExists(this IServiceBusNamespace sbNamespace, string name)
        {
            var topic = await sbNamespace.GetTopicByName(name);
            if (topic != null) return topic;

            topic = await sbNamespace.Topics
                             .Define(name.ToLower())
                             .WithDuplicateMessageDetection(TimeSpan.FromMinutes(10))
                             .CreateAsync();

            await sbNamespace.RefreshAsync();
            return topic;
        }

        /// <summary>
        /// Creates a specific subscription to a topic if it doesn't exist yet.
        /// </summary>
        /// <param name="topic">The <see cref="ITopic"/> that we are subscribing to.</param>
        /// <param name="name">The name of the subscription we are doing on the <see cref="ITopic"/>.</param>
        /// <returns>The <see cref="ISubscription"/> entity object that references the subscription.</returns>
        public static async Task<ISubscription> CreateSubscriptionIfNotExists(this ITopic topic, string name)
        {
            var subscription = await topic.GetSubscriptionByName(name);
            if (subscription != null) return subscription;

            subscription = await topic.Subscriptions
                       .Define(name.ToLower())
                       .WithMessageLockDurationInSeconds(60)
                       .WithExpiredMessageMovedToDeadLetterSubscription()
                       .WithMessageMovedToDeadLetterSubscriptionOnMaxDeliveryCount(10)
                       .CreateAsync();

            await topic.RefreshAsync();
            return subscription;
        }

        /// <summary>
        /// Parses a ServiceBus connection string to retrieve the name of the Namespace.
        /// </summary>
        /// <param name="connectionString">The connection string that we want to parse.</param>
        /// <returns>The name of the Namespace in the connection string.</returns>
        public static string GetNamespaceNameFromConnectionString(this string connectionString)
        {
            return Regex.Match(connectionString, @"Endpoint=sb:\/\/([^.]*)", RegexOptions.IgnoreCase).Groups[1].Value;
        }

        /// <summary>
        /// Gets a topic by name, will return null if not found.
        /// More efficient than enumerating all topics and find by name.
        /// </summary>
        /// <param name="sbNamespace">The sb namespace to extend.</param>
        /// <param name="name">The name of the topic to find.</param>
        /// <returns>The <see cref="ITopic"/> if exists and null if not.</returns>
        public static async Task<ITopic> GetTopicByName(this IServiceBusNamespace sbNamespace, string name)
        {
            try
            {
                return await sbNamespace.Topics.GetByNameAsync(name);
            }
            catch (Microsoft.Rest.Azure.CloudException ex)
                when (ex.Response.StatusCode == HttpStatusCode.NotFound)
            {
                // Not found.
                return null;
            }
        }

        /// <summary>
        /// Gets a queue by name, will return null if not found.
        /// More efficient than enumerating all queues and finding by name.
        /// </summary>
        /// <param name="sbNamespace">The sb namespace to extend.</param>
        /// <param name="name">The name of the topic to find.</param>
        /// <returns>The <see cref="ITopic"/> if exists and null if not.</returns>
        public static async Task<IQueue> GetQueueByName(this IServiceBusNamespace sbNamespace, string name)
        {
            try
            {
                return await sbNamespace.Queues.GetByNameAsync(name);
            }
            catch (Microsoft.Rest.Azure.CloudException ex)
                when (ex.Response.StatusCode == HttpStatusCode.NotFound)
            {
                // Not found.
                return null;
            }
        }

        /// <summary>
        /// Gets a topic subscription by name, will return null if not found.
        /// More efficient than enumerating all subscriptions and finding by name.
        /// </summary>
        /// <param name="topic">The topic to extend.</param>
        /// <param name="name">The name of the subscription to find.</param>
        /// <returns>The <see cref="ITopic"/> if exists and null if not.</returns>
        public static async Task<ISubscription> GetSubscriptionByName(this ITopic topic, string name)
        {
            try
            {
                return await topic.Subscriptions.GetByNameAsync(name);
            }
            catch (Microsoft.Rest.Azure.CloudException ex)
                when (ex.Response.StatusCode == HttpStatusCode.NotFound)
            {
                // Not found.
                return null;
            }
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
            var queueName = GetQueueNameForType(type);

            if (Debugger.IsAttached)
            {
                queueName += $"-{Environment.UserName.Replace("$", "")}";
            }

            return queueName?.ToLower();
#else
            return GetQueueNameForType(type)?.ToLower();
#endif
        }

        private static string GetQueueNameForType(Type type)
        {
            var attributeEventName = type.GetCustomAttribute<EswEventNameAttribute>()?.EventName;
            return string.IsNullOrWhiteSpace(attributeEventName) ? type.FullName : attributeEventName;
        }
    }
}

using System;
using System.Diagnostics;
using System.Net;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Eshopworld.Core;
using Microsoft.Azure.Management.ServiceBus.Fluent;
using Microsoft.Rest.Azure;

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
            var queue = await sbNamespace.GetQueueByNameAsync(name); 
            if (queue != null) return queue;
            
            try
            {
                return await sbNamespace.Queues
                             .Define(name.ToLowerInvariant())
                             .WithMessageLockDurationInSeconds(60)
                             .WithDuplicateMessageDetection(TimeSpan.FromMinutes(10))
                             .WithExpiredMessageMovedToDeadLetterQueue()
                             .WithMessageMovedToDeadLetterQueueOnMaxDeliveryCount(10)
                             .CreateAsync();
            }
            catch (CloudException ce)
                when (ce.Response.StatusCode == HttpStatusCode.BadRequest &&
                      ce.Message.Contains("SubCode=40000. The value for the requires duplicate detection property of an existing Queue cannot be changed"))
            {
                // Create queue race condition occurred. Return existing queue.
                return await sbNamespace.GetQueueByNameAsync(name);
            }
        }

        /// <summary>
        /// Creates a specific topic if it doesn't exist in the target namespace.
        /// </summary>
        /// <param name="sbNamespace">The <see cref="IServiceBusNamespace"/> where we are creating the topic in.</param>
        /// <param name="name">The name of the topic that we are looking for.</param>
        /// <returns>The <see cref="ITopic"/> entity object that references the Azure topic.</returns>
        public static async Task<ITopic> CreateTopicIfNotExists(this IServiceBusNamespace sbNamespace, string name)
        {
            var topic = await sbNamespace.GetTopicByNameAsync(name);
            if (topic != null) return topic;

            try
            {
                return await sbNamespace.Topics
                             .Define(name.ToLowerInvariant())
                             .WithDuplicateMessageDetection(TimeSpan.FromMinutes(10))
                             .CreateAsync();
            }
            catch (CloudException ce)
                when (ce.Response.StatusCode == HttpStatusCode.BadRequest && 
                      ce.Message.Contains("SubCode=40000. The value for the requires duplicate detection property of an existing Topic cannot be changed"))
            {
                // Create topic race condition occurred. Return existing topic.
                return await sbNamespace.GetTopicByNameAsync(name);
            }
        }

        /// <summary>
        /// Creates a specific subscription to a topic if it doesn't exist yet.
        /// </summary>
        /// <param name="topic">The <see cref="ITopic"/> that we are subscribing to.</param>
        /// <param name="name">The name of the subscription we are doing on the <see cref="ITopic"/>.</param>
        /// <returns>The <see cref="ISubscription"/> entity object that references the subscription.</returns>
        public static async Task<ISubscription> CreateSubscriptionIfNotExists(this ITopic topic, string name)
        {
            var subscription = await topic.GetSubscriptionByNameAsync(name);
            if (subscription != null) return subscription;

            return await topic.Subscriptions
                   .Define(name.ToLowerInvariant())
                   .WithMessageLockDurationInSeconds(60)
                   .WithExpiredMessageMovedToDeadLetterSubscription()
                   .WithMessageMovedToDeadLetterSubscriptionOnMaxDeliveryCount(10)
                   .CreateAsync();

            // No error handling is required for race conditions creating topic subscriptions - when the create is called, if
            // already exists, the existing subscription is returned with the create call.  Different behaviour from the creation
            // of topics or queues.
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
        public static async Task<ITopic> GetTopicByNameAsync(this IServiceBusNamespace sbNamespace, string name)
        {
            try
            {
                return await sbNamespace.Topics.GetByNameAsync(name.ToLowerInvariant());
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
        public static async Task<IQueue> GetQueueByNameAsync(this IServiceBusNamespace sbNamespace, string name)
        {
            try
            {
                return await sbNamespace.Queues.GetByNameAsync(name.ToLowerInvariant());
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
        public static async Task<ISubscription> GetSubscriptionByNameAsync(this ITopic topic, string name)
        {
            try
            {
                return await topic.Subscriptions.GetByNameAsync(name.ToLowerInvariant());
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

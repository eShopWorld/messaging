using System;
using System.Threading.Tasks;
using Eshopworld.Core;

namespace Eshopworld.Messaging.Core
{
    /// <summary>
    /// Contract that provides a simple way to send events to topics
    /// <remarks>
    /// Custom topic name needs to be provided.
    /// </remarks>
    /// </summary>
    public interface IPublishEventsWithTopicName: IDisposable
    {
        /// <summary>
        /// Sends an event onto a topic.
        /// </summary>
        /// <typeparam name="T">The type of the event that we are sending.</typeparam>
        /// <param name="event">The event that we are sending.</param>
        /// <param name="topicName">The name of the topic we are sending to</param>
        Task Publish<T>(T @event, string topicName) where T : class;
    }

    /// <summary>
    /// Contract that provides a simple way to send and receive events through callbacks.
    /// <remarks>
    /// Custom topic name needs to be provided.
    /// </remarks>
    /// </summary>
    public interface IDoPubSubWithTopicName: IPublishEventsWithTopicName, IMessageOperations
    {
        /// <summary>
        /// Sets up a call back for receiving any event of type <typeparamref name="T"/> for topic name <paramref name="topicName"/>.
        /// If you try to setup more then one callback to the same topic name <paramref name="topicName"/> you'll get an <see cref="InvalidOperationException"/>.
        /// </summary>
        /// <typeparam name="T">The type of the event that we are subscribing to receiving.</typeparam>
        /// <param name="callback">The <see cref="Action{T}"/> delegate that will be called for each event received.</param>
        /// <param name="subscriptionName">The name of the reliable subscription we're doing for this event type.</param>
        /// <param name="topicName">The name of the topic the subscription is created for</param>
        /// <param name="batchSize">The size of the batch when reading for a topic subscription - used as the pre-fetch parameter of the message receiver</param>
        /// <exception cref="InvalidOperationException">Thrown when you attempt to setup multiple callbacks against the same <paramref name="topicName"/>.</exception>
        Task Subscribe<T>(
            Action<T> callback, 
            string subscriptionName, 
            string topicName,
            int batchSize = 10) where T : class;
    }

    /// <summary>
    /// Contract that exposes all messaging operations, both for queues and topics.
    /// <remarks>For topics a custom topic name needs to be provided</remarks>
    /// </summary>
    public interface IMessagingWithTopicName: IDoMessages, IDoPubSubWithTopicName
    {
    }
}

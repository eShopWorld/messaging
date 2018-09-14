namespace Eshopworld.Messaging
{
    using Core;
    using JetBrains.Annotations;
    using Microsoft.Azure.ServiceBus.Core;
    using System;
    using System.Collections.Generic;
    using System.Reactive;
    using System.Reactive.Linq;
    using System.Reactive.Subjects;
    using System.Threading.Tasks;

    /// <summary>
    /// The main messenger entry point.
    /// It is recommended that only one messenger exists per application so you should always give this a singleton lifecycle.
    /// </summary>
    /// <remarks>
    /// Some checks are only valuable if you have a unique messenger class in your application. If you set it up as transient
    /// then some checks will only run against a specific instance and won't really validate the entire behaviour.
    /// </remarks>
    public class Messenger : IDoFullMessaging, IDoFullReactiveMessaging
    {
        internal readonly object Gate = new object();
        internal readonly string ConnectionString;
        internal readonly string SubscriptionId;

        internal readonly ISubject<object> MessagesIn = new Subject<object>();

        internal Dictionary<Type, IDisposable> MessageSubs = new Dictionary<Type, IDisposable>();
        internal Dictionary<Type, ServiceBusAdapter> ServiceBusAdapters = new Dictionary<Type, ServiceBusAdapter>();

        internal ServiceBusAdapter<T> GetQueueAdapterIfExists<T>() where T : class =>
            ServiceBusAdapters.TryGetValue(typeof(T), out var result)
                ? (ServiceBusAdapter<T>)result
                : throw new InvalidOperationException($"Messages/events of type {typeof(T).FullName} haven't been setup properly yet");

        /// <summary>
        /// Initializes a new instance of <see cref="Messenger"/>.
        /// </summary>
        /// <param name="connectionString">The Azure Service Bus connection string.</param>
        /// <param name="subscriptionId">The subscription ID where the service bus namespace lives.</param>
        public Messenger([NotNull]string connectionString, [NotNull]string subscriptionId)
        {
            ConnectionString = connectionString;
            SubscriptionId = subscriptionId;
        }

        /// <inheritdoc />
        public async Task Send<T>(T message)
            where T : class
        {
            if (!ServiceBusAdapters.ContainsKey(typeof(T)))
            {
                SetupMessageType<T>(10, MessagingTransportEnum.Queue);
            }

            await ((QueueAdapter<T>)ServiceBusAdapters[typeof(T)]).Send(message).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task Publish<T>(T @event)
            where T : class
        {
            if (!ServiceBusAdapters.ContainsKey(typeof(T)))
            {
                SetupMessageType<T>(10, MessagingTransportEnum.Topic);
            }

            await ((TopicAdapter<T>)ServiceBusAdapters[typeof(T)]).Send(@event).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public void Receive<T>(Action<T> callback, int batchSize = 10)
            where T : class
        {
            if (MessageSubs.ContainsKey(typeof(T)))
            {
                throw new InvalidOperationException("You already added a callback to this message type. Only one callback per type is supported.");
            }

            ((QueueAdapter<T>) SetupMessageType<T>(batchSize, MessagingTransportEnum.Queue)).StartReading();
            MessageSubs.Add(typeof(T), MessagesIn.OfType<T>().Subscribe(callback));
        }

        /// <inheritdoc />
        public async Task Subscribe<T>(Action<T> callback, string subscriptionName, int batchSize = 10)
            where T : class
        {
            if (MessageSubs.ContainsKey(typeof(T)))
            {
                throw new InvalidOperationException("You already added a callback to this message type. Only one callback per type is supported.");
            }

            await ((TopicAdapter<T>) SetupMessageType<T>(batchSize, MessagingTransportEnum.Topic)).StartReading(subscriptionName);
            MessageSubs.Add(typeof(T), MessagesIn.OfType<T>().Subscribe(callback));
        }

        /// <inheritdoc />
        public void CancelReceive<T>()
            where T : class
        {
            lock (Gate)
            {
                GetQueueAdapterIfExists<T>().StopReading();

                if (MessageSubs.ContainsKey(typeof(T))) // if reactive messenger is used, the subscriptions are handled by the package client
                {
                    MessageSubs[typeof(T)].Dispose();
                    MessageSubs.Remove(typeof(T));
                }
            }
        }

        /// <inheritdoc />
        public IObservable<T> GetMessageObservable<T>(int batchSize = 10) where T : class // GetMessageObservable || GetEventObservable
        {
            ((QueueAdapter<T>)SetupMessageType<T>(batchSize, MessagingTransportEnum.Queue)).StartReading();
            return MessagesIn.OfType<T>().AsObservable();
        }

        /// <inheritdoc />
        public async Task<IObservable<T>> GetEventObservable<T>(string subscriptionName, int batchSize = 10) where T : class
        {
            await ((TopicAdapter<T>)SetupMessageType<T>(batchSize, MessagingTransportEnum.Topic)).StartReading(subscriptionName);
            return MessagesIn.OfType<T>().AsObservable();
        }

        /// <inheritdoc />
        public async Task Lock<T>(T message) where T : class => await GetQueueAdapterIfExists<T>().Lock(message).ConfigureAwait(false);

        /// <inheritdoc />
        public async Task Complete<T>(T message) where T : class => await GetQueueAdapterIfExists<T>().Complete(message).ConfigureAwait(false);

        /// <inheritdoc />
        public async Task Abandon<T>(T message) where T : class => await GetQueueAdapterIfExists<T>().Abandon(message).ConfigureAwait(false);

        /// <inheritdoc />
        public async Task Error<T>(T message) where T : class => await GetQueueAdapterIfExists<T>().Error(message).ConfigureAwait(false);

        /// <inheritdoc />
        public void SetBatchSize<T>(int batchSize) where T : class => GetQueueAdapterIfExists<T>().SetBatchSize(batchSize);

        /// <summary>
        /// Sets the messenger up for either sending or receiving a specific message of type <typeparamref name="T"/>.
        /// This will create the <see cref="ServiceBusAdapter"/> but will not set it up for reading the queue.
        /// </summary>
        /// <typeparam name="T">The type of the message we are setting up.</typeparam>
        /// <param name="batchSize">The size of the batch when reading for a queue - used as the pre-fetch parameter of the <see cref="MessageReceiver"/>.</param>
        /// <param name="transport">The type of the transport to setup</param>
        /// <returns>The message queue adapter.</returns>
        internal ServiceBusAdapter<T> SetupMessageType<T>(int batchSize, MessagingTransportEnum transport) where T : class
        {
            ServiceBusAdapter adapter = null;
            lock (Gate)
            {
                if (!ServiceBusAdapters.ContainsKey(typeof(T)))
                {
                    switch (transport)
                    {
                        case MessagingTransportEnum.Queue:
                            adapter = new QueueAdapter<T>(ConnectionString, SubscriptionId, MessagesIn.AsObserver(), batchSize);
                            break;
                        case MessagingTransportEnum.Topic:
                            adapter = new TopicAdapter<T>(ConnectionString, SubscriptionId, MessagesIn.AsObserver(), batchSize);
                            break;
                        default:
                            throw new InvalidOperationException($"The {nameof(MessagingTransportEnum)} was extended and the use case on the {nameof(SetupMessageType)} switch wasn't.");
                    }

                    ServiceBusAdapters.Add(typeof(T), adapter);
                }
            }

            return (ServiceBusAdapter<T>)adapter ?? (ServiceBusAdapter<T>)ServiceBusAdapters[typeof(T)];
        }

        /// <inheritdoc />
        public void Dispose()
        {
            MessageSubs.Release();
            MessageSubs = null;

            ServiceBusAdapters.Release();
            ServiceBusAdapters = null;
        }
    }
}

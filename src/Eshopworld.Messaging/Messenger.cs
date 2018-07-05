namespace Eshopworld.Messaging
{
    using System;
    using System.Collections.Generic;
    using System.Reactive;
    using System.Reactive.Linq;
    using System.Reactive.Subjects;
    using System.Threading.Tasks;
    using Core;
    using JetBrains.Annotations;

    /// <summary>
    /// The main messenger entry point.
    /// It is recommended that only one messenger exists per application so you should always give this a singleton lifecycle.
    /// </summary>
    /// <remarks>
    /// Some checks are only valuable if you have a unique messenger class in your application. If you set it up as transient
    /// then some checks will only run against a specific instance and won't really validate the entire behaviour.
    /// </remarks>
    public class Messenger : IMessenger, IReactiveMessenger
    {
        internal readonly object Gate = new object();
        internal readonly string ConnectionString;
        internal readonly string SubscriptionId;

        internal readonly ISubject<object> MessagesIn = new Subject<object>();

        internal Dictionary<Type, IDisposable> MessageSubs = new Dictionary<Type, IDisposable>();
        internal Dictionary<Type, MessageQueueAdapter> QueueAdapters = new Dictionary<Type, MessageQueueAdapter>();

        internal MessageQueueAdapter<T> GetQueueAdapterIfExists<T>() where T : class =>
            QueueAdapters.TryGetValue(typeof(T), out var result)
                ? (MessageQueueAdapter<T>) result
                : throw new InvalidOperationException($"Messages of type {typeof(T).FullName} haven't been setup properly yet");

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
            if (!QueueAdapters.ContainsKey(typeof(T)))
            {
                SetupMessageType<T>(10);
            }

            await ((MessageQueueAdapter<T>)QueueAdapters[typeof(T)]).Send(message).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public void Receive<T>(Action<T> callback, int batchSize = 10)
            where T : class
        {
            lock (Gate)
            {
                if (MessageSubs.ContainsKey(typeof(T)))
                {
                    throw new InvalidOperationException("You already added a callback to this message type. Only one callback per type is supported.");
                }

                SetupMessageType<T>(batchSize).StartReading();

                MessageSubs.Add(
                    typeof(T),
                    MessagesIn.OfType<T>().Subscribe(callback));
            }
        }

        /// <inheritdoc />
        public void CancelReceive<T>()
            where T : class
        {
            lock (Gate)
            {
                GetQueueAdapterIfExists<T>().StopReading();

                MessageSubs[typeof(T)].Dispose();
                MessageSubs.Remove(typeof(T));
            }
        }

        /// <inheritdoc />
        public IObservable<T> GetObservable<T>(int batchSize = 10) where T : class
        {
            SetupMessageType<T>(batchSize).StartReading();

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
        /// This will create the <see cref="MessageQueueAdapter"/> but will not set it up for reading the queue.
        /// </summary>
        /// <typeparam name="T">The type of the message we are setting up.</typeparam>
        /// <param name="batchSize">The size of the batch when reading for a queue - used as the pre-fetch parameter of the </param>
        /// <returns>The message queue adapter.</returns>
        internal MessageQueueAdapter<T> SetupMessageType<T>(int batchSize) where T : class
        {
            lock (Gate)
            {
                if (!QueueAdapters.ContainsKey(typeof(T)))
                {
                    var queue = new MessageQueueAdapter<T>(ConnectionString, SubscriptionId, MessagesIn.AsObserver(), batchSize);
                    QueueAdapters.Add(typeof(T), queue);
                }
            }

            return (MessageQueueAdapter<T>)QueueAdapters[typeof(T)];
        }

        /// <inheritdoc />
        public void Dispose()
        {
            MessageSubs.Release();
            MessageSubs = null;

            QueueAdapters.Release();
            QueueAdapters = null;
        }
    }
}

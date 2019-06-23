using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading.Tasks;
using Eshopworld.Core;
using JetBrains.Annotations;
using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Microsoft.Azure.Management.ServiceBus.Fluent;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Rest;

namespace Eshopworld.Messaging
{
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
        internal readonly string NamespaceName;

        internal readonly ISubject<object> MessagesIn = new Subject<object>();

        internal ConcurrentDictionary<Type, IDisposable> MessageSubs = new ConcurrentDictionary<Type, IDisposable>();
        internal ConcurrentDictionary<Type, ServiceBusAdapter> ServiceBusAdapters = new ConcurrentDictionary<Type, ServiceBusAdapter>();

        internal IServiceBusNamespace AzureServiceBusNamespace;

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
            NamespaceName = ConnectionString.GetNamespaceNameFromConnectionString();
        }

        /// <inheritdoc />
        public async Task Send<T>(T message)
            where T : class
        {
            if (!ServiceBusAdapters.ContainsKey(typeof(T)))
            {
                SetupMessageType<T>(10, MessagingTransport.Queue);
            }

            await ((QueueAdapter<T>)ServiceBusAdapters[typeof(T)]).Send(message).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task Publish<T>(T @event)
            where T : class
        {
            if (!ServiceBusAdapters.ContainsKey(typeof(T)))
            {
                SetupMessageType<T>(10, MessagingTransport.Topic);
            }

            await ((TopicAdapter<T>)ServiceBusAdapters[typeof(T)]).Send(@event).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public void Receive<T>(Action<T> callback, int batchSize = 10)
            where T : class
        {
            if (!MessageSubs.TryAdd(typeof(T), MessagesIn.OfType<T>().Subscribe(callback)))
            {
                throw new InvalidOperationException("You already added a callback to this message type. Only one callback per type is supported.");
            }

            ((QueueAdapter<T>)SetupMessageType<T>(batchSize, MessagingTransport.Queue)).StartReading();
        }

        /// <inheritdoc />
        public async Task Subscribe<T>(Action<T> callback, string subscriptionName, int batchSize = 10)
            where T : class
        {
            if (!MessageSubs.TryAdd(typeof(T), MessagesIn.OfType<T>().Subscribe(callback)))
            {
                throw new InvalidOperationException("You already added a callback to this message type. Only one callback per type is supported.");
            }

            await ((TopicAdapter<T>)SetupMessageType<T>(batchSize, MessagingTransport.Topic)).StartReading(subscriptionName);
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
                    MessageSubs.TryRemove(typeof(T), out _);
                }
            }
        }

        /// <inheritdoc />
        public IObservable<T> GetMessageObservable<T>(int batchSize = 10) where T : class // GetMessageObservable || GetEventObservable
        {
            ((QueueAdapter<T>)SetupMessageType<T>(batchSize, MessagingTransport.Queue)).StartReading();
            return MessagesIn.OfType<T>().AsObservable();
        }

        /// <inheritdoc />
        public async Task<IObservable<T>> GetEventObservable<T>(string subscriptionName, int batchSize = 10) where T : class
        {
            await ((TopicAdapter<T>)SetupMessageType<T>(batchSize, MessagingTransport.Topic)).StartReading(subscriptionName);
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

        internal IServiceBusNamespace GetRefreshedServiceBusNamespace()
        {
            try
            {
                if (AzureServiceBusNamespace != null) return AzureServiceBusNamespace.Refresh();
            }
            catch{ /* soak */ }

            var token = new AzureServiceTokenProvider().GetAccessTokenAsync("https://management.core.windows.net/", string.Empty).Result;
            var tokenCredentials = new TokenCredentials(token);

            var client = RestClient.Configure()
                .WithEnvironment(AzureEnvironment.AzureGlobalCloud)
                .WithLogLevel(HttpLoggingDelegatingHandler.Level.Basic)
                .WithCredentials(new AzureCredentials(tokenCredentials, tokenCredentials, string.Empty, AzureEnvironment.AzureGlobalCloud))
                .Build();

            AzureServiceBusNamespace = Azure.Authenticate(client, string.Empty)
                .WithSubscription(SubscriptionId)
                .ServiceBusNamespaces.List()
                .SingleOrDefault(n => n.Name == NamespaceName);

            if (AzureServiceBusNamespace == null)
            {
                throw new InvalidOperationException($"Couldn't find the service bus namespace {NamespaceName} in the subscription with ID {SubscriptionId}");
            }

            return AzureServiceBusNamespace;
        }

        /// <summary>
        /// Sets the messenger up for either sending or receiving a specific message of type <typeparamref name="T"/>.
        /// This will create the <see cref="ServiceBusAdapter"/> but will not set it up for reading the queue.
        /// </summary>
        /// <typeparam name="T">The type of the message we are setting up.</typeparam>
        /// <param name="batchSize">The size of the batch when reading for a queue - used as the pre-fetch parameter of the <see cref="MessageReceiver"/>.</param>
        /// <param name="transport">The type of the transport to setup</param>
        /// <returns>The message queue adapter.</returns>
        internal ServiceBusAdapter<T> SetupMessageType<T>(int batchSize, MessagingTransport transport) where T : class
        {
            ServiceBusAdapter adapter = null;
            var messageType = typeof(T);

            if (!ServiceBusAdapters.ContainsKey(typeof(T)))
            {
                switch (transport)
                {
                    case MessagingTransport.Queue:
                        adapter = new QueueAdapter<T>(ConnectionString, SubscriptionId, MessagesIn.AsObserver(), batchSize, this);
                        break;
                    case MessagingTransport.Topic:
                        adapter = new TopicAdapter<T>(ConnectionString, SubscriptionId, MessagesIn.AsObserver(), batchSize, this);
                        break;
                    default:
                        throw new InvalidOperationException($"The {nameof(MessagingTransport)} was extended and the use case on the {nameof(SetupMessageType)} switch wasn't.");
                }

                if (!ServiceBusAdapters.TryAdd(typeof(T), adapter))
                {
                    adapter.Dispose();
                }
            }

            return (ServiceBusAdapter<T>)adapter ?? (ServiceBusAdapter<T>)ServiceBusAdapters[typeof(T)];
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                MessageSubs.Release();
                MessageSubs = null;

                ServiceBusAdapters.Release();
                ServiceBusAdapters = null;
            }
        }
    }
}

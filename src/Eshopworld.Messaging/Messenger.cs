using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading.Tasks;
using Eshopworld.Core;
using Eshopworld.Messaging.Core;
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
    public class Messenger : IDoFullMessaging, IDoFullReactiveMessaging, IMessagingWithTopicName
    {
        internal readonly object Gate = new object();
        internal readonly string ConnectionString;
        internal readonly string SubscriptionId;
        internal readonly string NamespaceName;

        internal readonly ISubject<object> MessagesIn = new Subject<object>();

        internal ConcurrentDictionary<string, IDisposable> MessageSubs = new ConcurrentDictionary<string, IDisposable>();
        internal ConcurrentDictionary<string, ServiceBusAdapter> ServiceBusAdapters = new ConcurrentDictionary<string, ServiceBusAdapter>();

        internal IServiceBusNamespace AzureServiceBusNamespace;

        internal ServiceBusAdapter<T> GetQueueAdapterIfExists<T>() where T : class =>
            ServiceBusAdapters.TryGetValue(GetTypeName<T>(), out var result)
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
            if (!ServiceBusAdapters.ContainsKey(GetTypeName<T>()))
            {
                SetupMessageType<T>(10, MessagingTransport.Queue);
            }

            await ((QueueAdapter<T>)ServiceBusAdapters[GetTypeName<T>()]).Send(message).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task Publish<T>(T @event)
            where T : class
        {
            if (!ServiceBusAdapters.ContainsKey(GetTypeName<T>()))
            {
                SetupMessageType<T>(10, MessagingTransport.Topic);
            }

            await ((TopicAdapter<T>)ServiceBusAdapters[GetTypeName<T>()]).Send(@event).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task Publish<T>(T @event, string topicName) where T : class
        {
            CheckTopicName(topicName);
            if (!ServiceBusAdapters.ContainsKey(topicName))
            {
                SetupMessageType<T>(10, MessagingTransport.Topic, topicName);
            }
            
            await ((TopicAdapter<T>)ServiceBusAdapters[topicName]).Send(@event).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public void Receive<T>(Action<T> callback, int batchSize = 10)
            where T : class
        {
            if (!MessageSubs.TryAdd(GetTypeName<T>(), MessagesIn.OfType<T>().Subscribe(callback)))
            {
                throw new InvalidOperationException("You already added a callback to this message type. Only one callback per type is supported.");
            }

            ((QueueAdapter<T>)SetupMessageType<T>(batchSize, MessagingTransport.Queue)).StartReading();
        }

        /// <inheritdoc />
        public async Task Subscribe<T>(Action<T> callback, string subscriptionName, int batchSize = 10)
            where T : class
        {
            if (!MessageSubs.TryAdd(GetTypeName<T>(), MessagesIn.OfType<T>().Subscribe(callback)))
            {
                throw new InvalidOperationException("You already added a callback to this message type. Only one callback per type is supported.");
            }

            await ((TopicAdapter<T>)SetupMessageType<T>(batchSize, MessagingTransport.Topic)).StartReading(subscriptionName).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task Subscribe<T>(Action<T> callback, string subscriptionName, string topicName, int batchSize = 10) where T : class
        {
            CheckTopicName(topicName);
            if (!MessageSubs.TryAdd(topicName, MessagesIn.OfType<T>().Subscribe(callback)))
            {
                throw new InvalidOperationException("You already added a callback to this topic. Only one callback per topic is supported.");
            }

            await ((TopicAdapter<T>)SetupMessageType<T>(batchSize, MessagingTransport.Topic, topicName)).StartReading(subscriptionName).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public void CancelReceive<T>()
            where T : class
        {
            lock (Gate)
            {
                GetQueueAdapterIfExists<T>().StopReading();

                if (MessageSubs.ContainsKey(GetTypeName<T>())) // if reactive messenger is used, the subscriptions are handled by the package client
                {
                    MessageSubs[GetTypeName<T>()].Dispose();
                    MessageSubs.TryRemove(GetTypeName<T>(), out _);
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
            await ((TopicAdapter<T>)SetupMessageType<T>(batchSize, MessagingTransport.Topic)).StartReading(subscriptionName).ConfigureAwait(false);
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
        /// Attempts to refresh the stored <see cref="IServiceBusNamespace"/> fluent construct.
        ///     Will do a full rebuild if any type of failure occurs during the refresh.
        /// </summary>
        /// <returns>The refreshed <see cref="IServiceBusNamespace"/>.</returns>
        internal async Task<IServiceBusNamespace> GetRefreshedServiceBusNamespace()
        {
            try
            {
                if (AzureServiceBusNamespace != null) return await AzureServiceBusNamespace.RefreshAsync().ConfigureAwait(false);
            }
            catch{ /* soak */ }

            var token = await new AzureServiceTokenProvider().GetAccessTokenAsync("https://management.core.windows.net/", string.Empty).ConfigureAwait(false);
            var tokenCredentials = new TokenCredentials(token);

            var client = RestClient.Configure()
                                   .WithEnvironment(AzureEnvironment.AzureGlobalCloud)
                                   .WithLogLevel(HttpLoggingDelegatingHandler.Level.Basic)
                                   .WithCredentials(new AzureCredentials(tokenCredentials, tokenCredentials, string.Empty, AzureEnvironment.AzureGlobalCloud))
                                   .Build();

            AzureServiceBusNamespace = (await Azure.Authenticate(client, string.Empty)
                                                   .WithSubscription(SubscriptionId)
                                                   .ServiceBusNamespaces.ListAsync()
                                                   .ConfigureAwait(false))
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
        /// <param name="topicName">Topic name to be used to create the topic. If not provided the type <typeparam name="T"/> will be used</param>
        /// <returns>The message queue adapter.</returns>
        internal ServiceBusAdapter<T> SetupMessageType<T>(int batchSize, MessagingTransport transport,
            string topicName = null) where T : class
        {
            ServiceBusAdapter adapter = null;

            if (!ServiceBusAdapters.ContainsKey(topicName ?? GetTypeName<T>()))
            {
                switch (transport)
                {
                    case MessagingTransport.Queue:
                        adapter = new QueueAdapter<T>(ConnectionString, SubscriptionId, MessagesIn.AsObserver(),
                            batchSize, this);
                        break;
                    case MessagingTransport.Topic:
                        adapter = new TopicAdapter<T>(ConnectionString, SubscriptionId, MessagesIn.AsObserver(),
                            batchSize, this, topicName);
                        break;
                    default:
                        throw new InvalidOperationException(
                            $"The {nameof(MessagingTransport)} was extended and the use case on the {nameof(SetupMessageType)} switch wasn't.");
                }

                if (!ServiceBusAdapters.TryAdd(topicName ?? GetTypeName<T>(), adapter))
                {
                    adapter.Dispose();
                    adapter = null;
                }
            }

            return (ServiceBusAdapter<T>) adapter ??
                   (ServiceBusAdapter<T>) ServiceBusAdapters[topicName ?? GetTypeName<T>()];
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

        private static string GetTypeName<T>() => typeof(T).FullName ?? typeof(T).Name;
        private static void CheckTopicName(string topicName)
        {
            if (string.IsNullOrEmpty(topicName)) throw new ArgumentException($"{nameof(topicName)} cannot be empty");
        }
    }
}

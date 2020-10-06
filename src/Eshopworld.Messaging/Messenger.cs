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

        internal ConcurrentDictionary<string, IDisposable> MessageSubs = new ConcurrentDictionary<string, IDisposable>();
        internal ConcurrentDictionary<string, ServiceBusAdapter> ServiceBusAdapters = new ConcurrentDictionary<string, ServiceBusAdapter>();

        internal IServiceBusNamespace AzureServiceBusNamespace;

        internal ServiceBusAdapter GetQueueAdapterIfExists(string topicName) =>
            ServiceBusAdapters.TryGetValue(topicName, out var result)
                ? result
                : throw new InvalidOperationException($"Messages/events for {topicName} haven't been setup properly yet");

        private const int MinimumIdleDurationMinutes = 5;

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
                SetupMessageType<T>(10, MessagingTransport.Queue, GetTypeName<T>());
            }

            await ((QueueAdapter<T>)ServiceBusAdapters[GetTypeName<T>()]).Send(message).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task Publish<T>(T @event, string topicName = null) where T : class
        {
            topicName ??= GetTypeName<T>();
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

            ((QueueAdapter<T>)SetupMessageType<T>(batchSize, MessagingTransport.Queue, GetTypeName<T>())).StartReading();
        }

        /// <inheritdoc />
        public async Task Subscribe<T>(Action<T> callback, string subscriptionName, string topicName = null, int batchSize = 10, int? deleteOnIdleDurationInMinutes = null) where T : class
        {
            CheckIdleDuration(deleteOnIdleDurationInMinutes);
            topicName ??= GetTypeName<T>();
            if (!MessageSubs.TryAdd(topicName, MessagesIn.OfType<T>().Subscribe(callback)))
            {
                throw new InvalidOperationException("You already added a callback to this topic. Only one callback per topic is supported.");
            }

            await ((TopicAdapter<T>)SetupMessageType<T>(batchSize, MessagingTransport.Topic, topicName)).StartReading(subscriptionName, deleteOnIdleDurationInMinutes).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public void CancelReceive<T>(string topicName= null) where T : class
        {
            topicName ??= GetTypeName<T>();

            lock (Gate)
            {
                GetQueueAdapterIfExists(topicName).StopReading();

                if (MessageSubs.ContainsKey(topicName)) // if reactive messenger is used, the subscriptions are handled by the package client
                {
                    MessageSubs[topicName].Dispose();
                    MessageSubs.TryRemove(topicName, out _);
                }
            }
        }

        /// <inheritdoc />
        public IObservable<T> GetMessageObservable<T>(int batchSize = 10) where T : class // GetMessageObservable || GetEventObservable
        {
            ((QueueAdapter<T>)SetupMessageType<T>(batchSize, MessagingTransport.Queue, GetTypeName<T>())).StartReading();
            return MessagesIn.OfType<T>().AsObservable();
        }

        /// <inheritdoc />
        public async Task<IObservable<T>> GetEventObservable<T>(string subscriptionName, int batchSize = 10) where T : class
        {
            await ((TopicAdapter<T>) SetupMessageType<T>(batchSize, MessagingTransport.Topic, GetTypeName<T>()))
                .StartReading(subscriptionName).ConfigureAwait(false);
            return MessagesIn.OfType<T>().AsObservable();
        }

        /// <inheritdoc />
        public async Task Lock<T>(T message, string topicName = null) where T : class
        {
            topicName ??= GetTypeName<T>();
            await GetQueueAdapterIfExists(topicName).Lock(message).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task Complete<T>(T message, string topicName = null) where T : class
        {
            topicName ??= GetTypeName<T>();
            await GetQueueAdapterIfExists(topicName).Complete(message).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task Abandon<T>(T message, string topicName = null) where T : class
        {
            topicName ??= GetTypeName<T>();
            await GetQueueAdapterIfExists(topicName).Abandon(message).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task Error<T>(T message, string topicName = null) where T : class
        {
            topicName ??= GetTypeName<T>();
            await GetQueueAdapterIfExists(topicName).Error(message).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public void SetBatchSize<T>(int batchSize, string topicName) where T : class
        {
            topicName ??= GetTypeName<T>();
            GetQueueAdapterIfExists(topicName).SetBatchSize(batchSize);
        }

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
        /// <param name="batchSize">The size of the batch when reading for a queue - used as the pre-fetch parameter of the <see cref="Microsoft.Azure.ServiceBus.Core.MessageReceiver"/>.</param>
        /// <param name="transport">The type of the transport to setup</param>
        /// <param name="topicName">Topic name to be used to create the topic. If not provided the type <typeparam name="T"/> will be used</param>
        /// <returns>The message queue adapter.</returns>
        internal ServiceBusAdapter<T> SetupMessageType<T>(int batchSize, MessagingTransport transport, string topicName)
            where T : class
        {
            ServiceBusAdapter adapter = null;

            if (!ServiceBusAdapters.ContainsKey(topicName))
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

                if (!ServiceBusAdapters.TryAdd(topicName, adapter))
                {
                    adapter.Dispose();
                    adapter = null;
                }
            }

            return (ServiceBusAdapter<T>) adapter ??
                   (ServiceBusAdapter<T>) ServiceBusAdapters[topicName];
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

        private static string GetTypeName<T>() => typeof(T).GetEntityName();

        private static void CheckIdleDuration(int? deleteOnIdleDurationInMinutes)
        {
            if (!deleteOnIdleDurationInMinutes.HasValue) return;

            if (deleteOnIdleDurationInMinutes < MinimumIdleDurationMinutes)
                throw new ArgumentException("Idle duration for subscription must be at least 5 minutes",
                    nameof(deleteOnIdleDurationInMinutes));
        }
    }
}

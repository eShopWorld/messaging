using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Azure.Management.ServiceBus.Fluent;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Newtonsoft.Json;

namespace Eshopworld.Messaging
{
    /// <summary>
    /// Generics based message queue router from <see cref="IObservable{T}"/> through to the <see cref="QueueClient"/>.
    /// </summary>
    /// <typeparam name="T">The type of the message that we are routing.</typeparam>
    internal sealed class TopicAdapter<T> : ServiceBusAdapter<T>
        where T : class
    {
        internal readonly string ConnectionString;
        internal readonly Messenger Messenger;
        internal readonly Type TopicType;
        internal ITopic AzureTopic;
        internal TopicClient Sender;
        internal ISubscription AzureTopicSubscription;
        internal string SubscriptionName;

        /// <summary>
        /// Initializes a new instance of <see cref="TopicAdapter{T}"/>.
        /// </summary>
        /// <param name="connectionString">The Azure Service Bus connection string.</param>
        /// <param name="subscriptionId">The ID of the subscription where the service bus namespace lives.</param>
        /// <param name="messagesIn">The <see cref="IObserver{IMessage}"/> used to push received messages into the pipeline.</param>
        /// <param name="batchSize">The size of the batch when reading for a queue - used as the pre-fetch parameter of the </param>
        /// <param name="messenger">The <see cref="Messenger"/> instance that created this adapter.</param>
        public TopicAdapter([NotNull]string connectionString, [NotNull]string subscriptionId, [NotNull]IObserver<T> messagesIn, int batchSize, Messenger messenger)
            : base(messagesIn, batchSize)
        {
            ConnectionString = connectionString;
            Messenger = messenger;

            TopicType = typeof(T);

            if (TopicType.FullName?.Length > 260) // SB quota: Entity path max length
            {
                throw new InvalidOperationException(
                    $@"You can't create queues for the type {TopicType.FullName} because the full name (namespace + name) exceeds 260 characters.
I suggest you reduce the size of the namespace: '{TopicType.Namespace}'.");
            }

            AzureTopic = Messenger.GetRefreshedServiceBusNamespace().CreateTopicIfNotExists(TopicType.GetEntityName()).Result;
            RebuildSender();
        }

        /// <summary>
        /// Starts pooling the queue in order to read messages in Peek Lock mode.
        /// </summary>
        /// <param name="subscriptionName">The name of the subscription that we want to read from.</param>
        internal async Task StartReading(string subscriptionName)
        {
            RebuildReceiver();

            AzureTopicSubscription = await GetRefreshedTopic().CreateSubscriptionIfNotExists(subscriptionName);
            LockInSeconds = AzureTopicSubscription.LockDurationInSeconds;
            LockTickInSeconds = (long)Math.Floor(LockInSeconds * 0.8); // renew at 80% to cope with load

            if (ReadTimer != null) return;

            ReadTimer = new Timer(
                async _ => await Read(_).ConfigureAwait(false),
                null,
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(1));

            SubscriptionName = subscriptionName;
        }

        /// <summary>
        /// Sends a single message.
        /// </summary>
        /// <param name="message">The message we want to send.</param>
        internal async Task Send([NotNull]T message)
        {
            var qMessage = new Message(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(message)))
            {
                ContentType = "application/json",
                Label = message.GetType().FullName
            };

            await SendPolicy.ExecuteAsync(async () =>
            {
                try
                {
                    await Sender.SendAsync(qMessage).ConfigureAwait(false);
                }
                catch
                {
                    RebuildSender();
                    throw;
                }
            });
        }

        /// <summary>
        /// Attempts to refresh the stored <see cref="ITopic"/> fluent construct.
        ///     Will do a full rebuild if any type of failure occurs during the refresh.
        /// </summary>
        /// <returns>The refreshed <see cref="ITopic"/>.</returns>
        internal ITopic GetRefreshedTopic()
        {
            try
            {
                if(AzureTopic != null) return AzureTopic.Refresh();
            }
            catch { /* soak */ }

            AzureTopic = Messenger.GetRefreshedServiceBusNamespace().CreateTopicIfNotExists(TopicType.GetEntityName()).Result;

            return AzureTopic;
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Sender.CloseAsync().Wait();
            }

            base.Dispose(disposing);
        }

        /// <inheritdoc />
        protected override void RebuildReceiver()
        {
            Receiver?.CloseAsync().Wait();
            Receiver = new MessageReceiver(ConnectionString, EntityNameHelper.FormatSubscriptionPath(AzureTopic.Name, SubscriptionName), ReceiveMode.PeekLock, new RetryExponential(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(500), 3), BatchSize);
        }

        /// <inheritdoc />
        protected override void RebuildSender()
        {
            Sender?.CloseAsync().Wait();
            Sender = new TopicClient(ConnectionString, AzureTopic.Name, new RetryExponential(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(500), 3));
        }
    }
}

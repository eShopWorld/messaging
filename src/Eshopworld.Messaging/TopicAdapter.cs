using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Azure.Management.ServiceBus.Fluent;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Newtonsoft.Json;
using Nito.AsyncEx;

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
        internal AsyncReaderWriterLock senderLock = new AsyncReaderWriterLock();

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

            AzureTopic = Messenger.GetRefreshedServiceBusNamespace().ConfigureAwait(false).GetAwaiter().GetResult()
                                  .CreateTopicIfNotExists(TopicType.GetEntityName()).ConfigureAwait(false).GetAwaiter().GetResult();
      
            RebuildSender().ConfigureAwait(false).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Starts pooling the queue in order to read messages in Peek Lock mode.
        /// </summary>
        /// <param name="subscriptionName">The name of the subscription that we want to read from.</param>
        internal async Task StartReading(string subscriptionName)
        {
            await RebuildReceiver().ConfigureAwait(false);

            AzureTopicSubscription = await (await GetRefreshedTopic().ConfigureAwait(false))
                                           .CreateSubscriptionIfNotExists(subscriptionName)
                                           .ConfigureAwait(false);

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

            await SendPolicy.ExecuteAsync(
                async () =>
                {
                    try
                    {
                        using (await senderLock.ReaderLockAsync())
                        {
                            await Sender.SendAsync(qMessage).ConfigureAwait(false);
                        }
                    }
                    catch
                    {
                        await RebuildSender().ConfigureAwait(false);
                        throw;
                    }
                }).ConfigureAwait(false);
        }

        /// <summary>
        /// Attempts to refresh the stored <see cref="ITopic"/> fluent construct.
        ///     Will do a full rebuild if any type of failure occurs during the refresh.
        /// </summary>
        /// <returns>The refreshed <see cref="ITopic"/>.</returns>
        internal async Task<ITopic> GetRefreshedTopic()
        {
            try
            {
                if(AzureTopic != null) return await AzureTopic.RefreshAsync().ConfigureAwait(false);
            }
            catch { /* soak */ }

            AzureTopic = await (await Messenger.GetRefreshedServiceBusNamespace()
                                               .ConfigureAwait(false))
                               .CreateTopicIfNotExists(TopicType.GetEntityName())
                               .ConfigureAwait(false);

            return AzureTopic;
        }

        /// <inheritdoc />
        protected override async Task RebuildReceiver()
        {
            using (await ReceiverLock.WriterLockAsync())
            {
                if (Receiver != null && !Receiver.IsClosedOrClosing)
                {
                    await Receiver.CloseAsync().ConfigureAwait(false);
                }

                Receiver = new MessageReceiver(ConnectionString, EntityNameHelper.FormatSubscriptionPath(AzureTopic.Name, SubscriptionName), ReceiveMode.PeekLock, new RetryExponential(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(500), 3), BatchSize);
            }
        }

        /// <inheritdoc />
        protected override async Task RebuildSender()
        {
            using (await senderLock.WriterLockAsync())
            {
                if (Sender != null && !Sender.IsClosedOrClosing)
                {
                    await Sender.CloseAsync().ConfigureAwait(false);
                }

                Sender = new TopicClient(ConnectionString, AzureTopic.Name, new RetryExponential(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(500), 3));
            }
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Sender.CloseAsync().ConfigureAwait(false).GetAwaiter().GetResult();
            }

            base.Dispose(disposing);
        }
    }
}

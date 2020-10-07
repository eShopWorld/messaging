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
        internal readonly string TopicName;
        internal ITopic AzureTopic;
        internal TopicClient Sender;
        internal ISubscription AzureTopicSubscription;
        internal string SubscriptionName;
        internal readonly AsyncReaderWriterLock SenderLock = new AsyncReaderWriterLock();

        /// <summary>
        /// Initializes a new instance of <see cref="TopicAdapter{T}"/>.
        /// </summary>
        /// <param name="connectionString">The Azure Service Bus connection string.</param>
        /// <param name="subscriptionId">The ID of the subscription where the service bus namespace lives.</param>
        /// <param name="messagesIn">The <see cref="IObserver{IMessage}"/> used to push received messages into the pipeline.</param>
        /// <param name="topicName">The topic name to use</param>
        /// <param name="batchSize">The size of the batch when reading for a queue - used as the pre-fetch parameter of the </param>
        /// <param name="messenger">The <see cref="Messenger"/> instance that created this adapter.</param>
        public TopicAdapter(
            [NotNull] string connectionString,
            [NotNull] string subscriptionId,
            [NotNull] IObserver<T> messagesIn,
            int batchSize,
            Messenger messenger,
            string topicName = null)
            : base(messagesIn, batchSize)
        {
            topicName = GetSafeTopicName(topicName);

            ConnectionString = connectionString;
            Messenger = messenger;
            TopicName = topicName;
            AzureTopic = Messenger.GetRefreshedServiceBusNamespace().ConfigureAwait(false).GetAwaiter().GetResult()
                .CreateTopicIfNotExists(TopicName).ConfigureAwait(false).GetAwaiter().GetResult();

            RebuildSender().ConfigureAwait(false).GetAwaiter().GetResult();
        }

        private static string GetSafeTopicName(string topicName) =>
            string.IsNullOrEmpty(topicName) ? CheckTopicType() : CheckTopicName(topicName);

        /// <summary>
        /// Starts pooling the queue in order to read messages in Peek Lock mode.
        /// </summary>
        /// <param name="subscriptionName">The name of the subscription that we want to read from.</param>
        /// <param name="deleteOnIdleDurationInMinutes">number of minutes for a subscription to be deleted when idle. If not provided then duration is infinite (TimeSpan.Max)</param>
        internal async Task StartReading(string subscriptionName, int? deleteOnIdleDurationInMinutes = null)
        {
            await RebuildReceiver().ConfigureAwait(false);

            AzureTopicSubscription = await (await GetRefreshedTopic().ConfigureAwait(false))
                .CreateSubscriptionIfNotExists(subscriptionName, deleteOnIdleDurationInMinutes)
                .ConfigureAwait(false);

            LockInSeconds = AzureTopicSubscription.LockDurationInSeconds;
            LockTickInSeconds = (long) Math.Floor(LockInSeconds * 0.8); // renew at 80% to cope with load

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
                Label = message.GetType().GetEntityName()
            };

            await SendPolicy.ExecuteAsync(
                async () =>
                {
                    try
                    {
                        using (await SenderLock.ReaderLockAsync())
                        {
                            await Sender.SendAsync(qMessage).ConfigureAwait(false);
                        }
                    }
                    /**
                     * we want to minimize the client rebuild frequency and ideally to reverse the approach
                     * rebuild only when there is valid reason to do so
                     * this list will need to be compiled/maintained
                     */
                    catch (QuotaExceededException) //let the polly deal with this - retry if allowed
                    {
                        throw;
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
                               .CreateTopicIfNotExists(TopicName)
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
            using (await SenderLock.WriterLockAsync())
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

        private static string CheckTopicName(string topicName)
        {
            if (topicName.Length > 260) // SB quota: Entity path max length
            {
                throw new InvalidOperationException(
                    $@"You can't create queues for the topic name {topicName} because it exceeds 260 characters.");
            }

            return topicName;
        }

        private static string CheckTopicType()
        {
            var topicType = typeof(T);
            var topicName = topicType.GetEntityName();

            if (topicName.Length > 260) // SB quota: Entity path max length
            {
                throw new InvalidOperationException(
                    $@"You can't create queues for the type {topicName} because the full name (namespace + name) exceeds 260 characters.
I suggest you reduce the size of the namespace: '{topicType.Namespace}'.");
            }

            return topicName;
        }
    }
}

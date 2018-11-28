namespace Eshopworld.Messaging
{
    using System;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using JetBrains.Annotations;
    using Microsoft.Azure.Management.ServiceBus.Fluent;
    using Microsoft.Azure.ServiceBus;
    using Microsoft.Azure.ServiceBus.Core;
    using Newtonsoft.Json;

    /// <summary>
    /// Generics based message queue router from <see cref="IObservable{T}"/> through to the <see cref="QueueClient"/>.
    /// </summary>
    /// <typeparam name="T">The type of the message that we are routing.</typeparam>
    internal sealed class TopicAdapter<T> : ServiceBusAdapter<T>
        where T : class
    {
        internal readonly ITopic AzureTopic;
        internal TopicClient Sender;
        internal ISubscription AzureSubscription;

        /// <summary>
        /// Initializes a new instance of <see cref="TopicAdapter{T}"/>.
        /// </summary>
        /// <param name="connectionString">The Azure Service Bus connection string.</param>
        /// <param name="subscriptionId">The ID of the subscription where the service bus namespace lives.</param>
        /// <param name="messagesIn">The <see cref="IObserver{IMessage}"/> used to push received messages into the pipeline.</param>
        /// <param name="batchSize">The size of the batch when reading for a queue - used as the pre-fetch parameter of the </param>
        /// <param name="typeOverride">The type override when we're creating a topic adapter for <see cref="string"/> types.</param>
        public TopicAdapter([NotNull]string connectionString, [NotNull]string subscriptionId, [NotNull]IObserver<T> messagesIn, int batchSize, Type typeOverride = null)
            : base(connectionString, subscriptionId, messagesIn, batchSize, typeOverride)
        {
            if (typeof(T) == typeof(Message) && typeOverride == null)
                throw new InvalidOperationException($"You can't create a TopicAdapter of type {typeof(Message).FullName} without specifying a typeOverride");

            if(typeof(T) != typeof(Message) && typeOverride != null)
                throw new InvalidOperationException($"typeOverride is only respected if you're creating a TopicAdapter where T:{typeof(Message).FullName}, and this one is for {typeof(T).FullName}");

            var topicType = typeOverride ?? typeof(T);
            AzureTopic = AzureServiceBusNamespace.CreateTopicIfNotExists(topicType.GetEntityName()).Result;
            Sender = new TopicClient(connectionString, AzureTopic.Name, new RetryExponential(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(500), 3));
        }

        /// <summary>
        /// Starts pooling the queue in order to read messages in Peek Lock mode.
        /// </summary>
        /// <param name="subscriptionName">The name of the subscription that we want to read from.</param>
        internal async Task StartReading(string subscriptionName)
        {
            if (Receiver != null)
            {
                try
                {
                    await Receiver.CloseAsync();
                }
                catch { /*soak*/ } // if it's already closed, Close() will throw. This is here for cases where StartReading is called in succession without calling StopReading.
            }

            Receiver = new MessageReceiver(ConnectionString, EntityNameHelper.FormatSubscriptionPath(AzureTopic.Name, subscriptionName), ReceiveMode.PeekLock, new RetryExponential(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(500), 3), BatchSize);

            AzureSubscription = await AzureTopic.CreateSubscriptionIfNotExists(subscriptionName);
            LockInSeconds = AzureSubscription.LockDurationInSeconds;
            LockTickInSeconds = (long)Math.Floor(LockInSeconds * 0.8); // renew at 80% to cope with load

            if (ReadTimer != null) return;

            ReadTimer = new Timer(
                async _ => await Read(_).ConfigureAwait(false),
                null,
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(1));
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
                Label = message.GetType().Name
            };

            await Sender.SendAsync(qMessage).ConfigureAwait(false);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Sender.CloseAsync().Wait();
            }

            base.Dispose(disposing);
        }
    }
}

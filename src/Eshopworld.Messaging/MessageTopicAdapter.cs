namespace Eshopworld.Messaging
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Core;
    using JetBrains.Annotations;
    using Microsoft.Azure.Management.ServiceBus.Fluent;
    using Microsoft.Azure.ServiceBus;
    using Microsoft.Azure.ServiceBus.Core;
    using Newtonsoft.Json;

    /// <summary>
    /// Generics based message queue router from <see cref="IObservable{T}"/> through to the <see cref="QueueClient"/>.
    /// </summary>
    /// <typeparam name="T">The type of the message that we are routing.</typeparam>
    internal sealed class MessageTopicAdapter<T> : ServiceBusAdapter
        where T : class
    {
        internal readonly ITopic AzureTopic;

        internal readonly MessageReceiver Receiver;
        internal readonly MessageSender Sender;

        internal readonly IDictionary<T, Message> Messages = new Dictionary<T, Message>(ObjectReferenceEqualityComparer<T>.Default);
        internal readonly IDictionary<T, Timer> LockTimers = new Dictionary<T, Timer>(ObjectReferenceEqualityComparer<T>.Default);
        internal readonly IObserver<T> MessagesIn;

        internal ISubscription AzureSubscription;
        internal long LockInSeconds;
        internal long LockTickInSeconds;

        internal Timer ReadTimer;
        internal int BatchSize = 10;

        /// <summary>
        /// Initializes a new instance of <see cref="MessageQueueAdapter{T}"/>.
        /// </summary>
        /// <param name="connectionString">The Azure Service Bus connection string.</param>
        /// <param name="subscriptionId">The ID of the subscription where the service bus namespace lives.</param>
        /// <param name="messagesIn">The <see cref="IObserver{IMessage}"/> used to push received messages into the pipeline.</param>
        /// <param name="batchSize">The size of the batch when reading for a queue - used as the pre-fetch parameter of the </param>
        public MessageTopicAdapter([NotNull]string connectionString, [NotNull]string subscriptionId, [NotNull]IObserver<T> messagesIn, int batchSize)
            : base(connectionString, subscriptionId, typeof(T))
        {
            MessagesIn = messagesIn;

            AzureTopic = AzureServiceBusNamespace.CreateTopicIfNotExists(typeof(T).GetEntityName()).Result;

            Receiver = new MessageReceiver(connectionString, AzureTopic.Name, ReceiveMode.PeekLock, new RetryExponential(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(500), 3), batchSize);
            Sender = new MessageSender(connectionString, AzureTopic.Name, new RetryExponential(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(500), 3));
        }

        /// <summary>
        /// Starts pooling the queue in order to read messages in Peek Lock mode.
        /// </summary>
        /// <param name="subscriptionName">The name of the subscription that we want to read from.</param>
        internal async Task StartReading(string subscriptionName)
        {
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
        /// Stops pooling the queue for reading messages.
        /// </summary>
        internal void StopReading()
        {
            ReadTimer.Dispose();
            ReadTimer = null;
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

            await Sender.SendAsync(qMessage).ConfigureAwait(false);
        }

        /// <summary>
        /// [BATCHED] Read message call back.
        /// </summary>
        /// <param name="_">[Ignored]</param>
        internal async Task Read([CanBeNull]object _)
        {
            var messages = await Receiver.ReceiveAsync(BatchSize).ConfigureAwait(false);
            if (messages == null) return;

            foreach (var message in messages)
            {
                var messageBody = JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(message.Body));

                Messages[messageBody] = message;
                MessagesIn.OnNext(messageBody);
            }
        }

        /// <summary>
        /// Creates a perpetual lock on a message by continuously renewing it's lock.
        /// This is usually created at the start of a handler so that we guarantee that we still have a valid lock
        /// and we retain that lock until we finish handling the message.
        /// </summary>
        /// <param name="message">The message that we want to create the lock on.</param>
        internal async Task Lock(T message)
        {
            await Receiver.RenewLockAsync(Messages[message]).ConfigureAwait(false);

            LockTimers.Add(
                message,
                new Timer(
                    async _ => { await Receiver.RenewLockAsync(Messages[message]).ConfigureAwait(false); },
                    null,
                    TimeSpan.FromSeconds(LockTickInSeconds),
                    TimeSpan.FromSeconds(LockTickInSeconds)));
        }

        /// <summary>
        /// Completes a message by doing the actual READ from the queue.
        /// </summary>
        /// <param name="message">The message we want to complete.</param>
        internal async Task Complete(T message)
        {
            await Receiver.CompleteAsync(Messages[message].SystemProperties.LockToken).ConfigureAwait(false);
            Release(message);
        }

        /// <summary>
        /// Abandons a message by returning it to the queue.
        /// </summary>
        /// <param name="message">The message we want to abandon.</param>
        internal async Task Abandon(T message)
        {
            await Receiver.AbandonAsync(Messages[message].SystemProperties.LockToken).ConfigureAwait(false);
            Release(message);
        }

        /// <summary>
        /// Errors a message by moving it specifically to the error queue.
        /// </summary>
        /// <param name="message">The message that we want to move to the error queue.</param>
        internal async Task Error(T message)
        {
            await Receiver.DeadLetterAsync(Messages[message].SystemProperties.LockToken).ConfigureAwait(false);
            Release(message);
        }

        /// <summary>
        /// Sets the size of the message batch during receives.
        /// </summary>
        /// <param name="batchSize">The size of the batch when reading for a queue - used as the pre-fetch parameter of the </param>
        internal void SetBatchSize(int batchSize)
        {
            BatchSize = batchSize;
            Receiver.PrefetchCount = batchSize;
        }

        /// <summary>
        /// Releases a message from the Queue by releasing all the specific message resources like lock
        /// renewal timers.
        /// This is called by all the methods that terminate the life of a message like COMPLETE, ABANDON and ERROR.
        /// </summary>
        /// <param name="message">The message that we want to release.</param>
        internal void Release([NotNull]T message)
        {
            lock (Gate)
            {
                Messages.Remove(message);

                // check for a lock renewal timer and release it if it exists
                if (LockTimers.ContainsKey(message))
                {
                    LockTimers[message]?.Dispose();
                    LockTimers.Remove(message);
                }
            }
        }

        /// <inheritdoc />
        public override void Dispose()
        {
            Receiver.CloseAsync().Wait();
            Sender.CloseAsync().Wait();

            ReadTimer?.Dispose();
        }
    }
}

namespace DevOpsFlex.Messaging
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Threading;
    using System.Threading.Tasks;
    using JetBrains.Annotations;
    using Microsoft.ServiceBus.Messaging;

    /// <summary>
    /// Generics based message queue router from <see cref="IObservable{IMessage}"/> through to the <see cref="QueueClient"/>.
    /// </summary>
    /// <typeparam name="T">The type of the message that we are routing.</typeparam>
    internal sealed class MessageQueue<T> : MessageQueue
        where T : IMessage
    {
        internal readonly IObserver<IMessage> MessagesIn;
        internal readonly IDisposable OutSubscription;

        internal Timer ReadTimer;

        /// <summary>
        /// Initializes a new instance of <see cref="MessageQueue{T}"/>.
        /// </summary>
        /// <param name="connectionString">The Azure Service Bus connection string.</param>
        /// <param name="messagesIn">The <see cref="IObserver{IMessage}"/> used to push received messages into the pipeline.</param>
        /// <param name="messagesOut">The <see cref="IObservable{IMessage}"/> used to take messages that are sent from the pipeline.</param>
        public MessageQueue([NotNull]string connectionString, [NotNull]IObserver<IMessage> messagesIn, [NotNull]IObservable<IMessage> messagesOut)
            : base(connectionString, typeof(T))
        {
            MessagesIn = messagesIn;

            OutSubscription = messagesOut.Subscribe(async m => await Send(m));
        }

        /// <summary>
        /// Starts pooling the queue in order to read messages in Peek Lock mode.
        /// </summary>
        internal void StartReading()
        {
            if (ReadTimer != null) return;

            ReadTimer = new Timer(
                async _ => await Read(_),
                null,
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(1)); // TODO: CREATE A DYNAMIC POOLING HEURISTIC
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
        /// <returns>The async <see cref="Task"/> wrapper.</returns>
        internal async Task Send([NotNull]IMessage message)
        {
#if DEBUG
            await QueueClient.SendAsync(new BrokeredMessage(message, new DataContractSerializer(typeof(T))));
#else
            await QueueClient.SendAsync(new BrokeredMessage(message));
#endif
        }

        /// <summary>
        /// [BATCHED] Read message call back.
        /// </summary>
        /// <param name="_">[Ignored]</param>
        /// <returns>The async <see cref="Task"/> wrapper.</returns>
        internal async Task Read([CanBeNull]object _)
        {
            var messages = await QueueClient.ReceiveBatchAsync(BatchSize);

            foreach (var message in messages)
            {
#if DEBUG
                var messageBody = message.GetBody<T>(new DataContractSerializer(typeof(T)));
#else
                var messageBody = message.GetBody<T>();
#endif

                BrokeredMessages.Add(messageBody, message);
                MessagesIn.OnNext(messageBody);
            }
        }

        /// <summary>
        /// Provides a mechanism for releasing resources.
        /// </summary>
        public override void Dispose()
        {
            OutSubscription?.Dispose();
            ReadTimer?.Dispose();

            base.Dispose();
        }
    }

    /// <summary>
    /// Non generic message queue router from <see cref="IObservable{IMessage}"/> through to the <see cref="QueueClient"/>.
    /// </summary>
    internal class MessageQueue : IDisposable
    {
        internal const int LockInSeconds = 60;

        internal static readonly IDictionary<IMessage, BrokeredMessage> BrokeredMessages = new Dictionary<IMessage, BrokeredMessage>();
        internal static readonly IDictionary<IMessage, Timer> LockTimers = new Dictionary<IMessage, Timer>();

        internal static int LockTickInSeconds = (int)Math.Floor(LockInSeconds * 0.6);

        internal static readonly object Gate = new object();
        protected readonly QueueClient QueueClient;

        internal int BatchSize = 10;

        /// <summary>
        /// Initializes a new instance of <see cref="MessageQueue"/>.
        /// </summary>
        /// <param name="connectionString">The Azure Service Bus connection string.</param>
        /// <param name="messageType">The fully strongly typed <see cref="Type"/> of the message we want to create the queue for.</param>
        internal MessageQueue([NotNull]string connectionString, [NotNull]Type messageType)
        {
            if (messageType.FullName.Length > 260) // SB quota: Entity path max length
            {
                throw new InvalidOperationException(
$@"You can't create queues for the type {messageType.FullName} because the full name (namespace + name) exceeds 260 characters.
I suggest you reduce the size of the namespace '{messageType.Namespace}'.");
            }

            if (messageType.FullName.ToLower() == ErrorQueue.ErrorQueueName) // Error queue clash
            {
                throw new InvalidOperationException($"Are you seriously creating a message named {ErrorQueue.ErrorQueueName} ???");
            }

            QueueClient = QueueCllientExtensions.CreateIfNotExists(connectionString, messageType.FullName).Result; // unwrapp
        }

        /// <summary>
        /// Releases a message from the Queue by releasing all the specific message resources like lock
        /// renewal timers.
        /// This is called by all the methods that terminate the life of a message like COMPLETE, ABANDON and ERROR.
        /// </summary>
        /// <param name="message">The message that we want to release.</param>
        internal static void Release(IMessage message)
        {
            lock (Gate)
            {
                BrokeredMessages[message]?.Dispose();
                BrokeredMessages.Remove(message);
            }

            lock (Gate)
            {

                // check for a lock renewal timer and release it if it exists
                if (LockTimers.ContainsKey(message))
                {
                    LockTimers[message]?.Dispose();
                    LockTimers.Remove(message);
                }
            }
        }

        /// <summary>
        /// Provides a mechanism for releasing resources.
        /// </summary>
        public virtual void Dispose()
        {
            try
            {
                QueueClient.Close();
            }
            catch { QueueClient.Abort(); }
        }
    }
}

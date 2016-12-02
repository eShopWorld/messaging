namespace DevOpsFlex.Messaging
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using JetBrains.Annotations;
    using Microsoft.ServiceBus.Messaging;

    internal sealed class MessageQueue<T> : MessageQueue
        where T : IMessage
    {
        internal readonly IObserver<IMessage> MessagesIn;
        internal readonly IDisposable OutSubscription;

        internal int BatchSize = 10;
        internal Timer ReadTimer;

        public MessageQueue([NotNull]string connectionString, [NotNull]IObserver<IMessage> messagesIn, [NotNull]IObservable<IMessage> messagesOut)
            : base(connectionString, typeof(T))
        {
            MessagesIn = messagesIn;

            OutSubscription = messagesOut.Subscribe(async m => await Send(m));
        }

        internal void StartReading()
        {
            if (ReadTimer != null) return;

            ReadTimer = new Timer(
                async _ => await Read(_),
                null,
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(1)); // TODO: CREATE A DYNAMIC POOLING HEURISTIC
        }

        internal void StopReading()
        {
            ReadTimer.Dispose();
            ReadTimer = null;
        }

        internal async Task Send([NotNull]IMessage message)
        {
            await QueueClient.SendAsync(new BrokeredMessage(message));
        }

        internal async Task Read([CanBeNull]object _)
        {
            var messages = await QueueClient.ReceiveBatchAsync(BatchSize);

            foreach (var message in messages)
            {
                var messageBody = message.GetBody<T>();

                BMessages.Add(messageBody, message);
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

    internal class MessageQueue : IDisposable
    {
        internal static readonly IDictionary<IMessage, BrokeredMessage> BMessages = new Dictionary<IMessage, BrokeredMessage>();

        protected readonly QueueClient QueueClient;

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
                throw new InvalidOperationException($"Are you seriously creating a message named {ErrorQueue.ErrorQueueName} ??? Seriously ???");
            }

            QueueClient = QueueCllientExtensions.CreateIfNotExists(connectionString, messageType.FullName).Result; // unwrapp
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

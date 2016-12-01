namespace DevOpsFlex.Messaging
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using JetBrains.Annotations;
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;

    internal sealed class MessageQueue<T> : MessageQueue
        where T : IMessage
    {
        internal int BatchSize = 10;

        internal readonly IObserver<IMessage> MessagesIn;
        internal readonly IDisposable OutSubscription;

        internal readonly Timer ReadTimer;

        public MessageQueue([NotNull]string connectionString, [NotNull]IObserver<IMessage> messagesIn, [NotNull]IObservable<IMessage> messagesOut)
            :base(connectionString, typeof(T))
        {
            MessagesIn = messagesIn;

            OutSubscription = messagesOut.Subscribe(async m => await Send(m));

            ReadTimer = new Timer(
                async _ => await Read(_),
                null,
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(1)); // TODO: CREATE A DYNAMIC POOLING HEURISTIC
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
                MessagesIn.OnNext(message.GetBody<T>());
            }
        }

        public override void Dispose()
        {
            OutSubscription?.Dispose();
            ReadTimer?.Dispose();

            base.Dispose();
        }
    }

    internal class MessageQueue : IDisposable
    {
        internal readonly QueueClient QueueClient;

        internal MessageQueue([NotNull]string connectionString, [NotNull]Type messageType)
        {
            if (messageType.FullName.Length > 260) // SB quota: Entity path max length
            {
                throw new InvalidOperationException(
$@"You can't create queues for the type {messageType.FullName} because the full name (namespace + name) exceeds 260 characters.
I suggest you reduce the size of the namespace '{messageType.Namespace}'.");
            }

            QueueClient = CreateIfNotExists(connectionString, messageType.FullName).Result; // unwrapp
        }

        [NotNull] internal async Task<QueueClient> CreateIfNotExists([NotNull]string connectionString, [NotNull]string entityPath)
        {
            var nsm = NamespaceManager.CreateFromConnectionString(connectionString);

            if (!await nsm.QueueExistsAsync(entityPath))
            {
                await nsm.CreateQueueAsync(entityPath); // TODO: MISSING QUEUE CREATION PROPERTIES
            }

            return QueueClient.CreateFromConnectionString(connectionString, entityPath, ReceiveMode.PeekLock);
        }

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

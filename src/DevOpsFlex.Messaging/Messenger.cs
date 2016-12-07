namespace DevOpsFlex.Messaging
{
    using System;
    using System.Collections.Generic;
    using System.Reactive;
    using System.Reactive.Linq;
    using System.Reactive.Subjects;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Transactions;
    using JetBrains.Annotations;

    /// <summary>
    /// The main messenger entry point.
    /// It is recommended that only one messenger exists per application so you should always give this a singleton lifecycle.
    /// </summary>
    /// <remarks>
    /// Some checks are only valuable if you have a unique messenger class in your application. If you set it up as transient
    /// then some checks will only run against a specific instance and won't really validate the entire behaviour.
    /// </remarks>
    public class Messenger : IMessenger
    {
        internal static readonly ISubject<IMessage> ErrorMessages = new Subject<IMessage>();

        internal readonly object Gate = new object();
        internal readonly string ConnectionString;

        internal readonly ISubject<IMessage> MessagesIn = new Subject<IMessage>();
        internal readonly ISubject<IMessage> MessagesOut = new Subject<IMessage>();

        internal readonly Dictionary<Type, IDisposable> MessageSubs = new Dictionary<Type, IDisposable>();
        internal readonly Dictionary<Type, MessageQueue> Queues = new Dictionary<Type, MessageQueue>();
        internal readonly ErrorQueue ErrorQueue;

        /// <summary>
        /// Initializes a new instance of <see cref="Messenger"/>.
        /// </summary>
        /// <param name="connectionString">The Azure Service Bus connection string.</param>
        public Messenger([NotNull]string connectionString)
        {
            ConnectionString = connectionString;
            ErrorQueue = new ErrorQueue(connectionString, ErrorMessages.AsObservable());
        }

        /// <summary>
        /// Sends a message.
        /// </summary>
        /// <typeparam name="T">The type of the message that we are sending.</typeparam>
        /// <param name="message">The message that we are sending.</param>
        public void Send<T>(T message)
            where T : IMessage
        {
            SetupMessageType<T>();
            MessagesOut.OnNext(message);
        }

        /// <summary>
        /// Sets up a call back for receiving any message of type <typeparamref name="T"/>.
        /// If you try to setup more then one callback to the same message type <see cref="T"/> you'll get an <see cref="InvalidOperationException"/>.
        /// </summary>
        /// <typeparam name="T">The type of the message that we are subscribing to receiving.</typeparam>
        /// <param name="callback">The <see cref="Action{T}"/> delegate that will be called for each message received.</param>
        /// <exception cref="InvalidOperationException">Thrown when you attempt to setup multiple callbacks against the same <see cref="T"/> parameter.</exception>
        public void Receive<T>(Action<T> callback)
            where T : IMessage
        {
            if (MessageSubs.ContainsKey(typeof(T)))
            {
                throw new InvalidOperationException("You already added a callback to this message type. Only one callback per type is supported.");
            }

            SetupMessageType<T>().StartReading();

            MessageSubs.Add(
                typeof(T),
                MessagesIn.OfType<T>().Subscribe(callback));
        }

        /// <summary>
        /// Sets the messenger up for either sending or receiving a specific message of type <typeparamref name="T"/>.
        /// This will create the <see cref="Microsoft.ServiceBus.Messaging.QueueClient"/> but will not set it up for reading the queue.
        /// </summary>
        /// <typeparam name="T">The type of the message we are setting up.</typeparam>
        /// <returns>The message queue wrapper.</returns>
        internal MessageQueue<T> SetupMessageType<T>()
            where T : IMessage
        {
            lock (Gate)
            {
                if (!Queues.ContainsKey(typeof(T)))
                {
                    var queue = new MessageQueue<T>(ConnectionString, MessagesIn.AsObserver(), MessagesOut.AsObservable());
                    Queues.Add(typeof(T), queue);
                }
            }

            return (MessageQueue<T>) Queues[typeof(T)];
        }

        /// <summary>
        /// Creates a perpetual lock on a message by continuously renewing it's lock.
        /// This is usually created at the start of a handler so that we guarantee that we still have a valid lock
        /// and we retain that lock until we finish handling the message.
        /// </summary>
        /// <param name="message">The message that we want to create the lock on.</param>
        /// <returns>The async <see cref="Task"/> wrapper</returns>
        internal static async Task Lock(IMessage message)
        {
            var bMessage = MessageQueue.BrokeredMessages[message];
            await bMessage.RenewLockAsync();

            MessageQueue.LockTimers.Add(
                message,
                new Timer(
                    _ =>
                    {
                        bMessage.RenewLockAsync();
                    },
                    null,
                    TimeSpan.FromSeconds(MessageQueue.LockTickInSeconds),
                    TimeSpan.FromSeconds(MessageQueue.LockTickInSeconds)));
        }

        /// <summary>
        /// Completes a message by doing the actual READ from the queue.
        /// </summary>
        /// <param name="message">The message we want to complete.</param>
        /// <returns>The async <see cref="Task"/> wrapper</returns>
        internal static async Task Complete(IMessage message)
        {
            await MessageQueue.BrokeredMessages[message].CompleteAsync();
            MessageQueue.Release(message);
        }

        /// <summary>
        /// Abandons a message by returning it to the queue.
        /// </summary>
        /// <param name="message">The message we want to abandon.</param>
        /// <returns>The async <see cref="Task"/> wrapper</returns>
        internal static async Task Abandon(IMessage message)
        {
            await MessageQueue.BrokeredMessages[message].AbandonAsync();
            MessageQueue.Release(message);
        }

        /// <summary>
        /// Errors a message by moving it specifically to the error queue.
        /// </summary>
        /// <param name="message">The message that we want to move to the error queue.</param>
        /// <returns>The async <see cref="Task"/> wrapper</returns>
        internal static async Task Error(IMessage message)
        {
            using (var scope = new TransactionScope())
            {
                ErrorMessages.OnNext(message);
                await MessageQueue.BrokeredMessages[message].CompleteAsync();

                scope.Complete();
            }

            MessageQueue.Release(message);
        }


        /// <summary>
        /// Provides a mechanism for releasing resources.
        /// </summary>
        public void Dispose()
        {
            // TODO: This guy needs to do a lot of work!

            ErrorQueue.Dispose();
        }
    }
}

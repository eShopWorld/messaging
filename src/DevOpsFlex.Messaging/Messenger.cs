namespace DevOpsFlex.Messaging
{
    using System;
    using System.Collections.Generic;
    using System.Reactive;
    using System.Reactive.Linq;
    using System.Reactive.Subjects;
    using System.Threading.Tasks;
    using JetBrains.Annotations;

    public class Messenger : IMessenger, IDisposable
    {
        private static readonly ISubject<IMessage> ErrorMessages = new Subject<IMessage>();

        private readonly object _gate = new object();
        private readonly string _connectionString;

        private readonly ISubject<IMessage> _messagesIn = new Subject<IMessage>();
        private readonly ISubject<IMessage> _messagesOut = new Subject<IMessage>();

        private readonly Dictionary<Type, IDisposable> _messageSubs = new Dictionary<Type, IDisposable>();
        private readonly Dictionary<Type, MessageQueue> _queues = new Dictionary<Type, MessageQueue>();
        private readonly ErrorQueue _errorQueue;

        public Messenger([NotNull]string connectionString)
        {
            _connectionString = connectionString;
            _errorQueue = new ErrorQueue(connectionString, ErrorMessages.AsObservable());
        }

        public void Send<T>(T message)
            where T : IMessage
        {
            SetupMessageType<T>();
            _messagesOut.OnNext(message);
        }

        /// <summary>
        /// 
        ///     If you try to setup more then one callback to the same message type <see cref="T"/> you'll get an <see cref="InvalidOperationException"/>.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="callback"></param>
        /// <exception cref="InvalidOperationException">Thrown when you attempt to setup multiple callbacks against the same <see cref="T"/> parameter.</exception>
        public void Receive<T>(Action<T> callback)
            where T : IMessage
        {
            if (_messageSubs.ContainsKey(typeof(T)))
            {
                throw new InvalidOperationException("You already added a callback to this message type. Only one callback per type is supported.");
            }

            SetupMessageType<T>().StartReading();

            _messageSubs.Add(
                typeof(T),
                _messagesIn.OfType<T>().Subscribe(callback));
        }

        internal MessageQueue<T> SetupMessageType<T>()
            where T : IMessage
        {
            lock (_gate)
            {
                if (!_queues.ContainsKey(typeof(T)))
                {
                    var queue = new MessageQueue<T>(_connectionString, _messagesIn.AsObserver(), _messagesOut.AsObservable());
                    _queues.Add(typeof(T), queue);
                }
            }

            return (MessageQueue<T>) _queues[typeof(T)];
        }
        internal static async Task Lock(IMessage message)
        {
            // TODO: MISSING VALIDATION

            await Task.Yield(); // TODO
        }

        internal static async Task Complete(IMessage message)
        {
            // TODO: MISSING VALIDATION

            await MessageQueue.BMessages[message].CompleteAsync();
        }

        internal static async Task Abandon(IMessage message)
        {
            // TODO: MISSING VALIDATION

            await MessageQueue.BMessages[message].AbandonAsync();
        }

        internal static async Task Error(IMessage message)
        {
            // TODO: MISSING VALIDATION

            ErrorMessages.OnNext(message);
            await MessageQueue.BMessages[message].CompleteAsync();
        }


        /// <summary>
        /// Provides a mechanism for releasing resources.
        /// </summary>
        public void Dispose()
        {
            // TODO: This guy needs to do a lot of work!
        }
    }
}

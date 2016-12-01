namespace DevOpsFlex.Messaging
{
    using System;
    using System.Collections.Generic;
    using System.Reactive;
    using System.Reactive.Linq;
    using System.Reactive.Subjects;
    using JetBrains.Annotations;

    public class Messenger : IMessenger
    {
        private readonly object _gate = new object();
        private readonly string _connectionString;

        private readonly ISubject<IMessage> _messagesIn = new Subject<IMessage>();
        private readonly ISubject<IMessage> _messagesOut = new Subject<IMessage>();

        private readonly Dictionary<Type, IDisposable> _messageSubs = new Dictionary<Type, IDisposable>();
        private readonly Dictionary<Type, MessageQueue> _queues = new Dictionary<Type, MessageQueue>();

        public Messenger([NotNull]string connectionString)
        {
            _connectionString = connectionString;
        }

        public void Send<T>(T message)
            where T : IMessage
        {
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

            lock (_gate)
            {
                if (!_queues.ContainsKey(typeof(T)))
                {
                    _queues.Add(
                        typeof(T),
                        new MessageQueue<T>(_connectionString, _messagesIn.AsObserver(), _messagesOut.AsObservable()));
                }

                _messageSubs.Add(
                    typeof(T),
                    _messagesIn.OfType<T>().Subscribe(callback));
            }
        }
    }
}

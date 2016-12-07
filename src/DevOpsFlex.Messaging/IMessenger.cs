namespace DevOpsFlex.Messaging
{
    using System;
    using JetBrains.Annotations;

    public interface IMessenger : IDisposable
    {
        /// <summary>
        /// Sends a message.
        /// </summary>
        /// <typeparam name="T">The type of the message that we are sending.</typeparam>
        /// <param name="message">The message that we are sending.</param>
        void Send<T>([NotNull]T message)
            where T : IMessage;

        /// <summary>
        /// Sets up a call back for receiving any message of type <typeparamref name="T"/>.
        /// If you try to setup more then one callback to the same message type <see cref="T"/> you'll get an <see cref="InvalidOperationException"/>.
        /// </summary>
        /// <typeparam name="T">The type of the message that we are subscribing to receiving.</typeparam>
        /// <param name="callback">The <see cref="Action{T}"/> delegate that will be called for each message received.</param>
        /// <exception cref="InvalidOperationException">Thrown when you attempt to setup multiple callbacks against the same <see cref="T"/> parameter.</exception>
        void Receive<T>([NotNull]Action<T> callback)
            where T : IMessage;
    }
}
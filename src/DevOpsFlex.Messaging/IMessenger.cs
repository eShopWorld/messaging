namespace DevOpsFlex.Messaging
{
    using System;
    using System.Threading.Tasks;
    using JetBrains.Annotations;

    /// <summary>
    /// Contract that provides a simple way to send messages.
    /// </summary>
    public interface ISendMessages : IDisposable
    {
        /// <summary>
        /// Sends a message.
        /// </summary>
        /// <typeparam name="T">The type of the message that we are sending.</typeparam>
        /// <param name="message">The message that we are sending.</param>
        Task Send<T>([NotNull] T message) where T : IMessage;
    }

    /// <summary>
    /// Contract that provides a simple way to send and receive messages through callbacks.
    /// </summary>
    public interface IMessenger : ISendMessages
    {
        /// <summary>
        /// Sets up a call back for receiving any message of type <typeparamref name="T"/>.
        /// If you try to setup more then one callback to the same message type <typeparamref name="T"/> you'll get an <see cref="InvalidOperationException"/>.
        /// </summary>
        /// <typeparam name="T">The type of the message that we are subscribing to receiving.</typeparam>
        /// <param name="callback">The <see cref="Action{T}"/> delegate that will be called for each message received.</param>
        /// <exception cref="InvalidOperationException">Thrown when you attempt to setup multiple callbacks against the same <typeparamref name="T"/> parameter.</exception>
        void Receive<T>([NotNull] Action<T> callback) where T : IMessage;
    }

    /// <summary>
    /// Contract that exposes a reactive way to receive and send messages.
    /// </summary>
    public interface IReactiveMessenger : ISendMessages
    {
        /// <summary>
        /// Setups up the required receive pipeline for the given message type and returns a reactive
        /// <see cref="IObservable{T}"/> that you can plug into.
        /// </summary>
        /// <typeparam name="T">The type of the message we want the reactive pipeline for.</typeparam>
        /// <returns>The typed <see cref="IObservable{T}"/> that you can plug into.</returns>
        IObservable<T> GetObservable<T>() where T : IMessage;
    }
}
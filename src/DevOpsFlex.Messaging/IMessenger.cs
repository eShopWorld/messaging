namespace DevOpsFlex.Messaging
{
    using System;
    using System.Threading.Tasks;
    using JetBrains.Annotations;

    /// <summary>
    /// Contract specifying the ability to do message operations like Complete and Abandon.
    /// </summary>
    public interface IMessageOperations
    {
        /// <summary>
        /// Stops receiving a message type by disabling the read pooling on the <see cref="MessageQueueAdapter"/>.
        /// </summary>
        /// <typeparam name="T">The type of the message that we are cancelling the receive on.</typeparam>
        void CancelReceive<T>() where T : class;

        /// <summary>
        /// Creates a perpetual lock on a message by continuously renewing it's lock.
        /// This is usually created at the start of a handler so that we guarantee that we still have a valid lock
        /// and we retain that lock until we finish handling the message.
        /// </summary>
        /// <param name="message">The message that we want to create the lock on.</param>
        /// <returns>The async <see cref="Task"/> wrapper</returns>
        Task Lock<T>(T message) where T : class;

        /// <summary>
        /// Completes a message by doing the actual READ from the queue.
        /// </summary>
        /// <param name="message">The message we want to complete.</param>
        /// <returns>The async <see cref="Task"/> wrapper</returns>

        Task Complete<T>(T message) where T : class;

        /// <summary>
        /// Abandons a message by returning it to the queue.
        /// </summary>
        /// <param name="message">The message we want to abandon.</param>
        /// <returns>The async <see cref="Task"/> wrapper</returns>
        Task Abandon<T>(T message) where T : class;

        /// <summary>
        /// Errors a message by moving it specifically to the error queue.
        /// </summary>
        /// <param name="message">The message that we want to move to the error queue.</param>
        /// <returns>The async <see cref="Task"/> wrapper</returns>
        Task Error<T>(T message) where T : class;
    }

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
        Task Send<T>([NotNull] T message) where T : class;
    }

    /// <summary>
    /// Contract that provides a simple way to send and receive messages through callbacks.
    /// </summary>
    public interface IMessenger : ISendMessages, IMessageOperations
    {
        /// <summary>
        /// Sets up a call back for receiving any message of type <typeparamref name="T"/>.
        /// If you try to setup more then one callback to the same message type <typeparamref name="T"/> you'll get an <see cref="InvalidOperationException"/>.
        /// </summary>
        /// <typeparam name="T">The type of the message that we are subscribing to receiving.</typeparam>
        /// <param name="callback">The <see cref="Action{T}"/> delegate that will be called for each message received.</param>
        /// <exception cref="InvalidOperationException">Thrown when you attempt to setup multiple callbacks against the same <typeparamref name="T"/> parameter.</exception>
        void Receive<T>([NotNull] Action<T> callback) where T : class;
    }

    /// <summary>
    /// Contract that exposes a reactive way to receive and send messages.
    /// </summary>
    public interface IReactiveMessenger : ISendMessages, IMessageOperations
    {
        /// <summary>
        /// Setups up the required receive pipeline for the given message type and returns a reactive
        /// <see cref="IObservable{T}"/> that you can plug into.
        /// </summary>
        /// <typeparam name="T">The type of the message we want the reactive pipeline for.</typeparam>
        /// <returns>The typed <see cref="IObservable{T}"/> that you can plug into.</returns>
        IObservable<T> GetObservable<T>() where T : class;
    }
}
namespace DevOpsFlex.Messaging
{
    using System.Threading.Tasks;

    /// <summary>
    /// Marks a class as a message to be serialized and deserialized from the body of a
    /// <see cref="Microsoft.ServiceBus.Messaging.BrokeredMessage"/>.
    /// </summary>
    public interface IMessage { }

    /// <summary>
    /// Fluent extension wrappers to support the very thin interface <see cref="IMessage"/> instead of
    /// a base abstract class and still retain a fluent API.
    /// </summary>
    public static class MessageExtensions
    {
        /// <summary>
        /// Ensures that from this point forward you always have a valid lock on the message.
        /// Used at the start of any message handling.
        /// </summary>
        /// <param name="message">The message that we want to LOCK.</param>
        /// <returns>The async <see cref="Task"/> void wrapper.</returns>
        public static async Task Lock(IMessage message)
        {
            await Messenger.Lock(message);
        }

        /// <summary>
        /// Completes the receive operation of a message and indicates that the message should be marked as processed and deleted.
        /// </summary>
        /// <param name="message">The message that we want to COMPLETE.</param>
        /// <returns>The async <see cref="Task"/> void wrapper.</returns>
        public static async Task Complete(IMessage message)
        {
            await Messenger.Complete(message);
        }

        /// <summary>
        /// Abandons a message by abandoning the lock on the peek-locked brokered message.
        /// </summary>
        /// <param name="message">The message that we want to ABANDON.</param>
        /// <returns>The async <see cref="Task"/> void wrapper.</returns>
        public static async Task Abandon(IMessage message)
        {
            await Messenger.Abandon(message);
        }

        /// <summary>
        /// Moves this message to the error queue, marking it for replayability when possible.
        /// </summary>
        /// <param name="message">The message that we want to ERROR.</param>
        /// <returns>The async <see cref="Task"/> void wrapper.</returns>
        public static async Task Error(IMessage message)
        {
            await Messenger.Error(message);
        }
    }
}

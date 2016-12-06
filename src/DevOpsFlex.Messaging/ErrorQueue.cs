namespace DevOpsFlex.Messaging
{
    using System;
    using System.Threading.Tasks;
    using JetBrains.Annotations;
    using Microsoft.ServiceBus.Messaging;

    /// <summary>
    /// This is a special queue used to error messages that have know detailed problems during
    /// handling.
    /// </summary>
    internal class ErrorQueue : IDisposable
    {
        internal const string ErrorQueueName = "error";
        internal readonly QueueClient QueueClient;
        private readonly IDisposable _errorSub;

        /// <summary>
        /// Initializes an instance of <see cref="ErrorQueue"/>
        /// </summary>
        /// <param name="connectionString">The Azure Service Bus connection string.</param>
        /// <param name="messagesIn">The <see cref="IObservable{IMessage}"/> used to stream messages that are to be sent to the error queue.</param>
        internal ErrorQueue([NotNull]string connectionString, [NotNull]IObservable<IMessage> messagesIn)
        {
            QueueClient = QueueCllientExtensions.CreateIfNotExists(connectionString, "error").Result; // unwrapp
            _errorSub = messagesIn.Subscribe(async m => await SendToError(m));
        }

        /// <summary>
        /// Sends a message to the error queue. It's a very simple entry to QueueClient send.
        /// </summary>
        /// <param name="message">The <see cref="IMessage"/> that we want to send to the error queue.</param>
        /// <returns>The async <see cref="Task"/> wrapper.</returns>
        internal async Task SendToError(IMessage message)
        {
            await QueueClient.SendAsync(new BrokeredMessage(message));
        }

        /// <summary>
        /// Provides a mechanism for releasing resources.
        /// </summary>
        public void Dispose()
        {
            _errorSub?.Dispose();

            try { QueueClient.Close(); }
            catch { QueueClient.Abort(); }
        }
    }
}

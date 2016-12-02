namespace DevOpsFlex.Messaging
{
    using System;
    using System.Threading.Tasks;
    using JetBrains.Annotations;
    using Microsoft.ServiceBus.Messaging;

    internal class ErrorQueue : IDisposable
    {
        internal const string ErrorQueueName = "error";
        internal readonly QueueClient QueueClient;
        private readonly IDisposable _errorSub;

        internal ErrorQueue([NotNull]string connectionString, [NotNull]IObservable<IMessage> messagesIn)
        {
            QueueClient = QueueCllientExtensions.CreateIfNotExists(connectionString, "error").Result; // unwrapp
            _errorSub = messagesIn.Subscribe(async m => await SendToError(m));
        }

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

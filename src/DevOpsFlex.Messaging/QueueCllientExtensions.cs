namespace DevOpsFlex.Messaging
{
    using System.Threading.Tasks;
    using JetBrains.Annotations;
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;

    /// <summary>
    /// Contains extension methods for <see cref="QueueClient"/>.
    /// </summary>
    public static class QueueCllientExtensions
    {
        /// <summary>
        /// Creates a <see cref="QueueClient"/> for a given queue.
        /// If the queue doesn't exist this will created it before instantiating the <see cref="QueueClient"/>.
        /// </summary>
        /// <param name="connectionString">The service bus connection string.</param>
        /// <param name="entityPath">The entity path for the queue we are connecting to.</param>
        /// <returns>The final <see cref="QueueClient"/>.</returns>
        [NotNull]internal static async Task<QueueClient> CreateIfNotExists([NotNull]string connectionString, [NotNull]string entityPath)
        {
            var nsm = NamespaceManager.CreateFromConnectionString(connectionString);

            if (!await nsm.QueueExistsAsync(entityPath))
            {
                await nsm.CreateQueueAsync(entityPath); // TODO: MISSING QUEUE CREATION PROPERTIES
            }

            return QueueClient.CreateFromConnectionString(connectionString, entityPath, ReceiveMode.PeekLock);
        }
    }
}

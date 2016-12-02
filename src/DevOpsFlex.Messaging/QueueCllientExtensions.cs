namespace DevOpsFlex.Messaging
{
    using System.Threading.Tasks;
    using JetBrains.Annotations;
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;

    public static class QueueCllientExtensions
    {
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

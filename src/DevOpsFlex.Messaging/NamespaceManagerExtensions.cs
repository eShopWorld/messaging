namespace DevOpsFlex.Messaging
{
    using System.Threading.Tasks;
    using Microsoft.ServiceBus;

    /// <summary>
    /// Contains extension methods for <see cref="NamespaceManager"/>.
    /// </summary>
    public static class NamespaceManagerExtensions
    {
        /// <summary>
        /// Scorches the entire namespace that the <see cref="NamespaceManager"/> is connected to.
        /// Current this wipes out all queues and topics. This is used mostly by integration tests, to guarantee that
        /// both queue and topic creation processes are in place and working as intended.
        /// </summary>
        /// <param name="nsm">The <see cref="NamespaceManager"/> we are using to scorch the namespace.</param>
        /// <returns>The async <see cref="Task"/> wrapper.</returns>
        public static async Task ScorchNamespace(this NamespaceManager nsm)
        {
            foreach (var queue in await nsm.GetQueuesAsync())
            {
                await nsm.DeleteQueueAsync(queue.Path);
            }

            foreach (var topic in await nsm.GetTopicsAsync())
            {
                await nsm.DeleteTopicAsync(topic.Path);
            }
        }
    }
}

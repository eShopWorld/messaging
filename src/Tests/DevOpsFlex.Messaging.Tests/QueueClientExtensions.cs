namespace DevOpsFlex.Messaging.Tests
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;
    using Microsoft.ServiceBus.Messaging;

    public static class QueueClientExtensions
    {
        public static async Task<IEnumerable<T>> MaterializeBatchAsync<T>(this QueueClient client, int count)
        {
            var rMessages = (await client.ReceiveBatchAsync(count)).ToList();
#if DEBUG
            return rMessages.Select(m => m.GetBody<T>(new DataContractSerializer(typeof(T))));
#else
            return rMessages.Select(m => m.GetBody<T>());
#endif
        }
    }
}

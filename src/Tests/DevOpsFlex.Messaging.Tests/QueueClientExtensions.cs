namespace DevOpsFlex.Messaging.Tests
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;
    using Microsoft.ServiceBus.Messaging;

    public static class QueueClientExtensions
    {
        public static async Task<IEnumerable<T>> ReadBatchAsync<T>(this QueueClient client, int count)
            where T : IMessage
        {
            var rMessages = (await client.ReceiveBatchAsync(count)).ToList();
#if DEBUG
            return rMessages.Select(m => m.GetBody<T>(new DataContractSerializer(typeof(T))));
#else
            return rMessages.Select(m => m.GetBody<T>());
#endif
        }

        public static async Task WriteBatchAsync<T>(this QueueClient client, IEnumerable<T> batch)
            where T : IMessage
        {
            foreach (var message in batch)
            {
#if DEBUG
                await client.SendAsync(new BrokeredMessage(message, new DataContractSerializer(typeof(T))));
#else
                await client.SendAsync(new BrokeredMessage(message));
#endif
            }
        }
    }
}

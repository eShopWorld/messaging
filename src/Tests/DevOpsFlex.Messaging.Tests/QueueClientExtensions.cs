namespace DevOpsFlex.Messaging.Tests
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Azure.ServiceBus;
    using Microsoft.Azure.ServiceBus.Core;
    using Newtonsoft.Json;

    public static class QueueClientExtensions
    {
        public static async Task<IEnumerable<T>> ReadBatchAsync<T>(this MessageReceiver receiver, int count)
        {
            var rMessages = (await receiver.ReceiveAsync(count)).ToList();
            return rMessages.Select(m => JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(m.Body)));
        }

        public static async Task WriteBatchAsync<T>(this MessageSender sender, IEnumerable<T> batch)
        {
            foreach (var message in batch)
            {
                var qMessage = new Message(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(message)))
                {
                    ContentType = "application/json",
                    Label = message.GetType().FullName
                };

                await sender.SendAsync(qMessage).ConfigureAwait(false);
            }
        }
    }
}

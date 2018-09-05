namespace Eshopworld.Messaging.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Azure.ServiceBus;
    using Microsoft.Azure.ServiceBus.Core;
    using Newtonsoft.Json;

    /// <summary>
    /// Contains test extensions for the several receive and send clients in the Azure Service Bus SDK.
    /// </summary>
    public static class MessageClientsExtensions
    {
        /// <summary>
        /// Reads a batch asynchronously directly through the Azure Service Bus SDK.
        ///     This is used in tests to short circuit package code so that we only use package
        ///     code on one direction of the test.
        /// </summary>
        /// <typeparam name="T">The <see cref="Type"/> of the message that we want to read the batch with.</typeparam>
        /// <param name="receiver">The <see cref="MessageReceiver"/> we are extending.</param>
        /// <param name="count">The size of the batch that we are reading.</param>
        /// <returns>The list of already deserialized messages pulled through the batch read.</returns>
        public static async Task<IEnumerable<T>> ReadBatchAsync<T>(this MessageReceiver receiver, int count)
        {
            var rMessages = (await receiver.ReceiveAsync(count)).ToList();
            return rMessages.Select(m => JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(m.Body)));
        }

        /// <summary>
        /// Writes a batch asynchronously directly through the Azure Service Bus SDK.
        ///     This is used in tests to short circuit package code so that we only use package
        ///     code on one direction of the test.
        /// </summary>
        /// <typeparam name="T">The <see cref="Type"/> of the message that we want to write the batch with.</typeparam>
        /// <param name="sender">The <see cref="MessageSender"/> we are extending.</param>
        /// <param name="batch">The list of messages that we are writing as a batch.</param>
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

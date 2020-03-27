using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Azure.Management.ServiceBus.Fluent;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Newtonsoft.Json;
using Nito.AsyncEx;

namespace Eshopworld.Messaging
{
    /// <summary>
    /// Generics based message queue router from <see cref="IObservable{T}"/> through to the <see cref="QueueClient"/>.
    /// </summary>
    /// <typeparam name="T">The type of the message that we are routing.</typeparam>
    internal sealed class QueueAdapter<T> : ServiceBusAdapter<T>
        where T : class
    {
        internal readonly string ConnectionString;
        internal readonly Messenger Messenger;
        internal readonly IQueue AzureQueue;

        internal MessageSender Sender;
        internal readonly AsyncReaderWriterLock SenderLock = new AsyncReaderWriterLock();

        /// <summary>
        /// Initializes a new instance of <see cref="QueueAdapter{T}"/>.
        /// </summary>
        /// <param name="connectionString">The Azure Service Bus connection string.</param>
        /// <param name="subscriptionId">The ID of the subscription where the service bus namespace lives.</param>
        /// <param name="messagesIn">The <see cref="IObserver{IMessage}"/> used to push received messages into the pipeline.</param>
        /// <param name="batchSize">The size of the batch when reading for a queue - used as the pre-fetch parameter of the </param>
        /// <param name="messenger">The <see cref="Messenger"/> instance that created this adapter.</param>
        public QueueAdapter([NotNull]string connectionString, [NotNull]string subscriptionId, [NotNull]IObserver<T> messagesIn, int batchSize, Messenger messenger)
            : base(messagesIn, batchSize)
        {
            ConnectionString = connectionString;
            Messenger = messenger;
            AzureQueue = Messenger.GetRefreshedServiceBusNamespace().ConfigureAwait(false).GetAwaiter().GetResult()
                                  .CreateQueueIfNotExists(typeof(T).GetEntityName()).ConfigureAwait(false).GetAwaiter().GetResult();

            LockInSeconds = AzureQueue.LockDurationInSeconds;
            LockTickInSeconds = (long) Math.Floor(LockInSeconds * 0.8); // renew at 80% to cope with load

            RebuildReceiver().ConfigureAwait(false).GetAwaiter().GetResult();
            RebuildSender().ConfigureAwait(false).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Starts pooling the queue in order to read messages in Peek Lock mode.
        /// </summary>
        internal void StartReading()
        {
            if (ReadTimer != null) return;

            ReadTimer = new Timer(
                async _ => await Read(_).ConfigureAwait(false),
                null,
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(1));
        }

        /// <summary>
        /// Sends a single message.
        /// </summary>
        /// <param name="message">The message we want to send.</param>
        internal async Task Send([NotNull]T message)
        {
            var qMessage = new Message(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(message)))
            {
                ContentType = "application/json",
                Label = message.GetType().GetEntityName()
            };

            await SendPolicy.ExecuteAsync(
                async () =>
                {
                    try
                    {
                        using (await SenderLock.ReaderLockAsync())
                        {
                            await Sender.SendAsync(qMessage).ConfigureAwait(false);
                        }
                    }
                    catch
                    {
                        await RebuildSender().ConfigureAwait(false);
                        throw;
                    }
                }).ConfigureAwait(false);
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Receiver.CloseAsync().ConfigureAwait(false).GetAwaiter().GetResult();
                Sender.CloseAsync().ConfigureAwait(false).GetAwaiter().GetResult();

                ReadTimer?.Dispose();
            }

            base.Dispose(disposing);
        }

        /// <inheritdoc />
        protected override async Task RebuildReceiver()
        {
            using (await ReceiverLock.WriterLockAsync())
            {
                if (Receiver != null && !Receiver.IsClosedOrClosing)
                {
                    await Receiver.CloseAsync().ConfigureAwait(false);
                }

                Receiver = new MessageReceiver(ConnectionString, AzureQueue.Name, ReceiveMode.PeekLock, new RetryExponential(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(500), 3), BatchSize);
            }
        }

        /// <inheritdoc />
        protected override async Task RebuildSender()
        {
            using (await SenderLock.WriterLockAsync())
            {
                if (Sender != null && !Sender.IsClosedOrClosing)
                {
                    await Sender.CloseAsync().ConfigureAwait(false);
                }

                Sender = new MessageSender(ConnectionString, AzureQueue.Name, new RetryExponential(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(500), 3));
            }
        }
    }
}

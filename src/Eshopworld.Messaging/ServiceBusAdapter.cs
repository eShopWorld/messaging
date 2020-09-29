using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Eshopworld.Core;
using JetBrains.Annotations;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Newtonsoft.Json;
using Nito.AsyncEx;
using Polly;
using Polly.Retry;

namespace Eshopworld.Messaging
{
    internal abstract class ServiceBusAdapter<T> : ServiceBusAdapter
        where T : class
    {
        internal readonly IDictionary<T, Message> Messages = new Dictionary<T, Message>(ObjectReferenceEqualityComparer<T>.Default);
        internal readonly IDictionary<T, Timer> LockTimers = new Dictionary<T, Timer>(ObjectReferenceEqualityComparer<T>.Default);
        internal readonly IObserver<T> MessagesIn;
        internal readonly bool RawMessages;

        internal MessageReceiver Receiver;
        internal AsyncReaderWriterLock ReceiverLock = new AsyncReaderWriterLock();

        internal Timer ReadTimer;
        internal int BatchSize;

        internal long LockInSeconds;
        internal long LockTickInSeconds;

        protected AsyncRetryPolicy SendPolicy = Policy.Handle<Exception>().RetryAsync(1);
        protected AsyncRetryPolicy ReceivePolicy = Policy.Handle<Exception>().RetryAsync(1);

        protected ServiceBusAdapter([NotNull]IObserver<T> messagesIn, int batchSize)
        {
            MessagesIn = messagesIn;
            BatchSize = batchSize;

            RawMessages = typeof(T) == typeof(Message);
        }

        /// <summary>
        /// Releases a message from the Queue by releasing all the specific message resources like lock
        /// renewal timers.
        /// This is called by all the methods that terminate the life of a message like COMPLETE, ABANDON and ERROR.
        /// </summary>
        /// <param name="message">The message that we want to release.</param>
        internal void Release([NotNull]T message)
        {
            lock (Gate)
            {
                if (!RawMessages)
                {
                    Messages.Remove(message);
                }

                // check for a lock renewal timer and release it if it exists
                if (LockTimers.ContainsKey(message))
                {
                    LockTimers[message]?.Dispose();
                    LockTimers.Remove(message);
                }
            }
        }
        
        /// <inheritdoc />
        internal override Task Complete<T1>(T1 message)
        {
            if (!(message is T tMessage))
                throw new ArgumentException($"{nameof(message)} should be of type {typeof(T)}");
            
            return Complete(tMessage);
        }

        /// <summary>
        /// Completes a message by doing the actual READ from the queue.
        /// </summary>
        /// <param name="message">The message we want to complete.</param>
        internal async Task Complete(T message)
        {
            var m = RawMessages ? message as Message : Messages[message];

            await ReceivePolicy.ExecuteAsync(
                async () =>
                {
                    try
                    {
                        using (await ReceiverLock.ReaderLockAsync())
                        {
                            await Receiver.CompleteAsync(m?.SystemProperties.LockToken).ConfigureAwait(false);
                        }
                    }
                    catch
                    {
                        await RebuildReceiver().ConfigureAwait(false);
                        throw;
                    }
                }).ConfigureAwait(false);

            Release(message);
        }

        /// <inheritdoc />
        internal override Task Abandon<T1>(T1 message)
        {
            if (!(message is T tMessage))
                throw new ArgumentException($"{nameof(message)} should be of type {typeof(T)}");
            
            return Abandon(tMessage);
        }

        /// <summary>
        /// Abandons a message by returning it to the queue.
        /// </summary>
        /// <param name="message">The message we want to abandon.</param>
        internal async Task Abandon(T message)
        {
            var m = RawMessages ? message as Message : Messages[message];

            await ReceivePolicy.ExecuteAsync(
                async () =>
                {
                    try
                    {
                        using (await ReceiverLock.ReaderLockAsync())
                        {
                            await Receiver.AbandonAsync(m?.SystemProperties.LockToken).ConfigureAwait(false);
                        }
                    }
                    catch
                    {
                        await RebuildReceiver().ConfigureAwait(false);
                        throw;
                    }
                }).ConfigureAwait(false);

            Release(message);
        }

        /// <inheritdoc />
        internal override Task Error<T1>(T1 message)
        {
            if (!(message is T tMessage))
                throw new ArgumentException($"{nameof(message)} should be of type {typeof(T)}");
            
            return Error(tMessage);
        }

        /// <summary>
        /// Errors a message by moving it specifically to the error queue.
        /// </summary>
        /// <param name="message">The message that we want to move to the error queue.</param>
        internal async Task Error(T message)
        {
            var m = RawMessages ? message as Message : Messages[message];

            await ReceivePolicy.ExecuteAsync(
                async () =>
                {
                    try
                    {
                        using (await ReceiverLock.ReaderLockAsync())
                        {
                            await Receiver.DeadLetterAsync(m?.SystemProperties.LockToken).ConfigureAwait(false);
                        }
                    }
                    catch
                    {
                        await RebuildReceiver().ConfigureAwait(false);
                        throw;
                    }
                }).ConfigureAwait(false);

            Release(message);
        }

        /// <inheritdoc />
        internal override Task Lock<T1>(T1 message)
        {
            if (!(message is T tMessage))
                throw new ArgumentException($"{nameof(message)} should be of type {typeof(T)}");
            
            return Lock(tMessage);
        }

        /// <summary>
        /// Creates a perpetual lock on a message by continuously renewing it's lock.
        /// This is usually created at the start of a handler so that we guarantee that we still have a valid lock
        /// and we retain that lock until we finish handling the message.
        /// </summary>
        /// <param name="message">The message that we want to create the lock on.</param>
        internal async Task Lock(T message)
        {
            var m = RawMessages ? message as Message : Messages[message];

            await ReceivePolicy.ExecuteAsync(
                async () =>
                {
                    try
                    {
                        using (await ReceiverLock.ReaderLockAsync())
                        {
                            await Receiver.RenewLockAsync(m).ConfigureAwait(false);
                        }
                    }
                    catch
                    {
                        await RebuildReceiver().ConfigureAwait(false);
                        throw;
                    }
                }).ConfigureAwait(false);

            LockTimers.Add(
                message,
                new Timer(
                    async _ => {
                        using (await ReceiverLock.ReaderLockAsync())
                        {
                            await Receiver.RenewLockAsync(Messages[message]).ConfigureAwait(false);
                        }
                    },
                    null,
                    TimeSpan.FromSeconds(LockTickInSeconds),
                    TimeSpan.FromSeconds(LockTickInSeconds)));
        }

        /// <summary>
        /// [BATCHED] Read message call back.
        /// </summary>
        /// <param name="_">[Ignored]</param>
        internal async Task Read([CanBeNull]object _)
        {
            if (Receiver.IsClosedOrClosing) return;
            IList<Message> messages = null;

            await ReceivePolicy.ExecuteAsync(
                async () =>
                {
                    try
                    {
                        using (await ReceiverLock.ReaderLockAsync())
                        {
                            messages = await Receiver.ReceiveAsync(BatchSize).ConfigureAwait(false);
                        }
                    }
                    catch
                    {
                        await RebuildReceiver().ConfigureAwait(false);
                        throw;
                    }
                }).ConfigureAwait(false);

            if (messages == null) return;

            foreach (var message in messages)
            {
                if (RawMessages)
                {
                    MessagesIn.OnNext(message as T);
                }
                else
                {
                    var messageBody = JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(message.Body));
                    Messages[messageBody] = message;
                    MessagesIn.OnNext(messageBody);
                }
            }
        }

        /// <inheritdoc />
        internal override void StopReading()
        {
            ReadTimer.Dispose();
            ReadTimer = null;
        }

        /// <inheritdoc />
        internal override void SetBatchSize(int batchSize)
        {
            BatchSize = batchSize;
            Receiver.PrefetchCount = batchSize;
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                ReadTimer?.Dispose();
                Receiver?.CloseAsync().ConfigureAwait(false).GetAwaiter().GetResult();
            }
        }

        /// <summary>
        /// Rebuild the receiver object used by the adapter implementation.
        ///     Used with retry policies to improve reliability of the receiver:
        ///     - On failure, rebuild and retry again right away
        ///     - If the retry fails -> throw
        /// </summary>
        protected abstract Task RebuildReceiver();

        /// <summary>
        /// Rebuild the sender object used by the adapter implementation.
        ///     Used with retry policies to improve reliability of the sender:
        ///     - On failure, rebuild and retry again right away
        ///     - If the retry fails -> throw
        /// </summary>
        protected abstract Task RebuildSender();
    }

    /// <summary>
    /// Non generic message queue/topic router from <see cref="IObservable{IMessage}"/> through to the ServiceBus entities.
    /// </summary>
    internal abstract class ServiceBusAdapter : IDisposable
    {
        internal readonly object Gate = new object();

        /// <inheritdoc />
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Completes a message by doing the actual READ from the queue.
        /// </summary>
        /// <param name="message">The message we want to complete.</param>
        internal abstract Task Complete<T>(T message) where T : class;

        /// <summary>
        /// Abandons a message by returning it to the queue.
        /// </summary>
        /// <param name="message">The message we want to abandon.</param>
        internal abstract Task Abandon<T>(T message);

        /// <summary>
        /// Errors a message by moving it specifically to the error queue.
        /// </summary>
        /// <param name="message">The message that we want to move to the error queue.</param>
        internal abstract Task Error<T>(T message);

        /// <summary>
        /// Sets the size of the message batch during receives.
        /// </summary>
        /// <param name="batchSize">The size of the batch when reading for a queue - used as the pre-fetch parameter of the </param>
        internal abstract void SetBatchSize(int batchSize);

        /// <summary>
        /// Stops pooling the queue for reading messages.
        /// </summary>
        internal abstract void StopReading();

        /// <summary>
        /// Creates a perpetual lock on a message by continuously renewing it's lock.
        /// This is usually created at the start of a handler so that we guarantee that we still have a valid lock
        /// and we retain that lock until we finish handling the message.
        /// </summary>
        /// <param name="message">The message that we want to create the lock on.</param>
        internal abstract Task Lock<T>(T message) where T : class;

        protected abstract void Dispose(bool disposing);
    }

    /// <summary>
    /// Represents the type of transport used within Azure Service Bus.
    /// </summary>
    internal enum MessagingTransport
    {
        Queue,
        Topic
    }
}

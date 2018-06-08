﻿namespace DevOpsFlex.Messaging
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text.RegularExpressions;
    using System.Threading;
    using System.Threading.Tasks;
    using JetBrains.Annotations;
    using Microsoft.Azure.Management.Fluent;
    using Microsoft.Azure.Management.ResourceManager.Fluent;
    using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
    using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
    using Microsoft.Azure.Management.ServiceBus.Fluent;
    using Microsoft.Azure.Services.AppAuthentication;
    using Microsoft.Rest;
    using Microsoft.ServiceBus.Messaging;

    /// <summary>
    /// Generics based message queue router from <see cref="IObservable{IMessage}"/> through to the <see cref="QueueClient"/>.
    /// </summary>
    /// <typeparam name="T">The type of the message that we are routing.</typeparam>
    internal sealed class MessageQueue<T> : MessageQueue
        where T : IMessage
    {
        internal readonly IObserver<IMessage> MessagesIn;
        internal Timer ReadTimer;

        /// <summary>
        /// Initializes a new instance of <see cref="MessageQueue{T}"/>.
        /// </summary>
        /// <param name="connectionString">The Azure Service Bus connection string.</param>
        /// <param name="messagesIn">The <see cref="IObserver{IMessage}"/> used to push received messages into the pipeline.</param>
        public MessageQueue([NotNull]string connectionString, [NotNull]IObserver<IMessage> messagesIn)
            : base(connectionString, typeof(T))
        {
            MessagesIn = messagesIn;
        }

        public MessageQueue([NotNull]string connectionString, [NotNull]string subscriptionId, [NotNull]IObserver<IMessage> messagesIn)
            : base(connectionString, subscriptionId, typeof(T))
        {
            MessagesIn = messagesIn;
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
        /// Stops pooling the queue for reading messages.
        /// </summary>
        internal void StopReading()
        {
            ReadTimer.Dispose();
            ReadTimer = null;
        }

        /// <summary>
        /// Sends a single message.
        /// </summary>
        /// <param name="message">The message we want to send.</param>
        /// <returns>The async <see cref="Task"/> wrapper.</returns>
        internal async Task Send([NotNull]IMessage message)
        {
#if DEBUG
            await QueueClient.SendAsync(new BrokeredMessage(message, new DataContractSerializer(typeof(T)))).ConfigureAwait(false);
#else
            await QueueClient.SendAsync(new BrokeredMessage(message)).ConfigureAwait(false);
#endif
        }

        /// <summary>
        /// [BATCHED] Read message call back.
        /// </summary>
        /// <param name="_">[Ignored]</param>
        /// <returns>The async <see cref="Task"/> wrapper.</returns>
        internal async Task Read([CanBeNull]object _)
        {
            var messages = await QueueClient.ReceiveBatchAsync(BatchSize).ConfigureAwait(false);

            foreach (var message in messages)
            {
#if DEBUG
                var messageBody = message.GetBody<T>(new DataContractSerializer(typeof(T)));
#else
                var messageBody = message.GetBody<T>();
#endif

                // we intentionally want to override the .Add check to avoid
                // writing more code to cope with message locks that have experied and are being read twice
                // before erroring.
                BrokeredMessages[messageBody] = message;
                MessagesIn.OnNext(messageBody);
            }
        }

        /// <summary>
        /// Provides a mechanism for releasing resources.
        /// </summary>
        public override void Dispose()
        {
            ReadTimer?.Dispose();
            base.Dispose();
        }
    }

    /// <summary>
    /// Non generic message queue router from <see cref="IObservable{IMessage}"/> through to the <see cref="QueueClient"/>.
    /// </summary>
    internal class MessageQueue : IDisposable
    {
        internal const int LockInSeconds = 60; // TODO: CHANGE THIS STUFF NOW THAT WE CAN EASILY POKE THE IQUEUE !!!

        internal static readonly object Gate = new object();
        internal static readonly int LockTickInSeconds = (int)Math.Floor(LockInSeconds * 0.6);
        internal static readonly IDictionary<IMessage, BrokeredMessage> BrokeredMessages = new Dictionary<IMessage, BrokeredMessage>(ObjectReferenceEqualityComparer<IMessage>.Default);
        internal static readonly IDictionary<IMessage, Timer> LockTimers = new Dictionary<IMessage, Timer>(ObjectReferenceEqualityComparer<IMessage>.Default);
        internal static IServiceBusNamespace AzureServiceBusNamespace;

        internal readonly IQueue AzureQueue;

        internal int BatchSize = 10; // TODO: EXPOSING THIS THROUGH API IS PART OF THE WORKLOAD
        protected readonly QueueClient QueueClient;

        /// <summary>
        /// Initializes a new instance of <see cref="MessageQueue"/>.
        /// </summary>
        /// <param name="connectionString">The Azure Service Bus connection string.</param>
        /// <param name="messageType">The fully strongly typed <see cref="Type"/> of the message we want to create the queue for.</param>
        internal MessageQueue([NotNull]string connectionString, [NotNull]Type messageType)
        {
            if (messageType.FullName?.Length > 260) // SB quota: Entity path max length
            {
                throw new InvalidOperationException(
$@"You can't create queues for the type {messageType.FullName} because the full name (namespace + name) exceeds 260 characters.
I suggest you reduce the size of the namespace: '{messageType.Namespace}'.");
            }

            QueueClient = QueueCllientExtensions.CreateIfNotExists(connectionString, messageType.GetQueueName()).Result; // unwrapp
        }

        internal MessageQueue([NotNull]string connectionString, [NotNull]string subscriptionId, [NotNull]Type messageType)
        {
            var namespaceName = Regex.Match(connectionString, @"Endpoint=sb:\/\/([^.]*)", RegexOptions.IgnoreCase).Groups[1].Value;

            if (messageType.FullName?.Length > 260) // SB quota: Entity path max length
            {
                throw new InvalidOperationException(
$@"You can't create queues for the type {messageType.FullName} because the full name (namespace + name) exceeds 260 characters.
I suggest you reduce the size of the namespace: '{messageType.Namespace}'.");
            }

            if (AzureServiceBusNamespace == null)
            {
                var token = new AzureServiceTokenProvider().GetAccessTokenAsync("https://management.core.windows.net/", string.Empty).Result;
                var tokenCredentials = new TokenCredentials(token);

                var client = RestClient.Configure()
                                       .WithEnvironment(AzureEnvironment.AzureGlobalCloud)
                                       .WithLogLevel(HttpLoggingDelegatingHandler.Level.Basic)
                                       .WithCredentials(new AzureCredentials(tokenCredentials, tokenCredentials, string.Empty, AzureEnvironment.AzureGlobalCloud))
                                       .Build();

                AzureServiceBusNamespace = Azure.Authenticate(client, string.Empty)
                                                .WithSubscription(subscriptionId)
                                                .ServiceBusNamespaces.List()
                                                .SingleOrDefault(n => n.Name == namespaceName);

                if (AzureServiceBusNamespace == null)
                {
                    throw new InvalidOperationException($"Couldn't find the service bus namespace {namespaceName} in the subscription with ID {subscriptionId}");
                }
            }

            AzureQueue = AzureServiceBusNamespace.CreateQueueIfNotExists(messageType.FullName?.ToLower()).Result;
        }

        /// <summary>
        /// Releases a message from the Queue by releasing all the specific message resources like lock
        /// renewal timers.
        /// This is called by all the methods that terminate the life of a message like COMPLETE, ABANDON and ERROR.
        /// </summary>
        /// <param name="message">The message that we want to release.</param>
        internal static void Release(IMessage message)
        {
            lock (Gate)
            {
                BrokeredMessages[message]?.Dispose();
                BrokeredMessages.Remove(message);

                // check for a lock renewal timer and release it if it exists
                if (LockTimers.ContainsKey(message))
                {
                    LockTimers[message]?.Dispose();
                    LockTimers.Remove(message);
                }
            }
        }

        /// <summary>
        /// Provides a mechanism for releasing resources.
        /// </summary>
        public virtual void Dispose()
        {
            try
            {
                QueueClient.Close();
            }
            catch { QueueClient.Abort(); }
        }
    }
}

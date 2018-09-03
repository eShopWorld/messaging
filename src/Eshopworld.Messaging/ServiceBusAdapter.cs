namespace Eshopworld.Messaging
{
    using System;
    using System.Linq;
    using System.Text.RegularExpressions;
    using JetBrains.Annotations;
    using Microsoft.Azure.Management.Fluent;
    using Microsoft.Azure.Management.ResourceManager.Fluent;
    using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
    using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
    using Microsoft.Azure.Management.ServiceBus.Fluent;
    using Microsoft.Azure.Services.AppAuthentication;
    using Microsoft.Rest;

    /// <summary>
    /// Non generic message queue/topic router from <see cref="IObservable{IMessage}"/> through to the ServiceBus entities.
    /// </summary>
    internal abstract class ServiceBusAdapter : IDisposable
    {
        internal static IServiceBusNamespace AzureServiceBusNamespace;

        internal readonly object Gate = new object();

        /// <summary>
        /// Initializes a new instance of <see cref="ServiceBusAdapter"/>.
        /// </summary>
        /// <param name="connectionString">The Azure Service Bus connection string.</param>
        /// <param name="subscriptionId">The ID of the subscription where the service bus namespace lives.</param>
        /// <param name="messageType">The fully strongly typed <see cref="Type"/> of the message we want to create the queue for.</param>
        internal ServiceBusAdapter([NotNull]string connectionString, [NotNull]string subscriptionId, [NotNull]Type messageType)
        {
            if (messageType.FullName?.Length > 260) // SB quota: Entity path max length
            {
                throw new InvalidOperationException(
                    $@"You can't create queues for the type {messageType.FullName} because the full name (namespace + name) exceeds 260 characters.
I suggest you reduce the size of the namespace: '{messageType.Namespace}'.");
            }

            var namespaceName = Regex.Match(connectionString, @"Endpoint=sb:\/\/([^.]*)", RegexOptions.IgnoreCase).Groups[1].Value;

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
        }

        /// <inheritdoc />
        public abstract void Dispose();
    }
}
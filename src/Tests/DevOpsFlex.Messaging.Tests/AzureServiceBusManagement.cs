namespace DevOpsFlex.Messaging.Tests
{
    using System;
    using System.Linq;
    using System.Text.RegularExpressions;
    using System.Threading.Tasks;
    using JetBrains.Annotations;
    using Microsoft.Azure.Management.Fluent;
    using Microsoft.Azure.Management.ResourceManager.Fluent;
    using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
    using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
    using Microsoft.Azure.Services.AppAuthentication;
    using Microsoft.Rest;

    /// <summary>
    /// Contains extension methods for <see cref="Microsoft.Azure.Management.Fluent"/> around ServiceBus.
    /// </summary>
    public static class AzureServiceBusManagement
    {
        /// <summary>
        /// Scorches the entire service bus namespace.
        /// Currently this wipes out all queues and topics. This is used mostly by integration tests, to guarantee that
        /// both queue and topic creation processes are in place and working as intended.
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="subscriptionId"></param>
        /// <returns>The async <see cref="Task"/> wrapper.</returns>
        public static async Task ScorchNamespace([NotNull]string connectionString, [NotNull]string subscriptionId)
        {
            var namespaceName = Regex.Match(connectionString, @"Endpoint=sb:\/\/([^.]*)", RegexOptions.IgnoreCase).Groups[1].Value;

            var token = await new AzureServiceTokenProvider().GetAccessTokenAsync("https://management.core.windows.net/", string.Empty);
            var tokenCredentials = new TokenCredentials(token);

            var client = RestClient.Configure()
                                   .WithEnvironment(AzureEnvironment.AzureGlobalCloud)
                                   .WithLogLevel(HttpLoggingDelegatingHandler.Level.Basic)
                                   .WithCredentials(new AzureCredentials(tokenCredentials, tokenCredentials, string.Empty, AzureEnvironment.AzureGlobalCloud))
                                   .Build();

            var sbNamespace = Azure.Authenticate(client, string.Empty)
                                   .WithSubscription(subscriptionId)
                                   .ServiceBusNamespaces.List()
                                   .SingleOrDefault(n => n.Name == namespaceName);

            if (sbNamespace == null)
            {
                throw new InvalidOperationException($"Couldn't find the service bus namespace {namespaceName} in the subscription with ID {subscriptionId}");
            }

            foreach (var queue in await sbNamespace.Queues.ListAsync())
            {
                await sbNamespace.Queues.DeleteByNameAsync(queue.Name);
            }

            foreach (var topic in await sbNamespace.Topics.ListAsync())
            {
                await sbNamespace.Topics.DeleteByNameAsync(topic.Name);
            }
        }
    }
}

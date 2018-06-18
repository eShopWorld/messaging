namespace DevOpsFlex.Messaging.Tests
{
    using System;
    using System.Linq;
    using System.Text.RegularExpressions;
    using Microsoft.Azure.KeyVault;
    using Microsoft.Azure.Management.Fluent;
    using Microsoft.Azure.Management.ResourceManager.Fluent;
    using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
    using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
    using Microsoft.Azure.Management.ServiceBus.Fluent;
    using Microsoft.Azure.Services.AppAuthentication;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.Configuration.AzureKeyVault;
    using Microsoft.Rest;
    using Xunit;

    public class AzureServiceBusFixture : IDisposable
    {
        internal const string KeyvaultUriName = "TEST_KEYVAULT_URI";
        internal readonly IServiceBusNamespace ServiceBusNamespace;
        internal readonly MessagingSettings ConfigSettings = new MessagingSettings();

        public AzureServiceBusFixture()
        {
            var keyvaultUri = (Environment.GetEnvironmentVariable(KeyvaultUriName, EnvironmentVariableTarget.Machine) ??
                               Environment.GetEnvironmentVariable(KeyvaultUriName, EnvironmentVariableTarget.User)) ??
                               Environment.GetEnvironmentVariable(KeyvaultUriName, EnvironmentVariableTarget.Process);

            var config = new ConfigurationBuilder().AddAzureKeyVault(
                                                       keyvaultUri,
                                                       new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(new AzureServiceTokenProvider().KeyVaultTokenCallback)),
                                                       new DefaultKeyVaultSecretManager())
                                                   .Build();

            config.GetSection("Messaging").Bind(ConfigSettings);

            var namespaceName = Regex.Match(ConfigSettings.ConnectionString, @"Endpoint=sb:\/\/([^.]*)", RegexOptions.IgnoreCase).Groups[1].Value;

            var token = new AzureServiceTokenProvider().GetAccessTokenAsync("https://management.core.windows.net/", string.Empty).Result;
            var tokenCredentials = new TokenCredentials(token);

            var client = RestClient.Configure()
                                   .WithEnvironment(AzureEnvironment.AzureGlobalCloud)
                                   .WithLogLevel(HttpLoggingDelegatingHandler.Level.Basic)
                                   .WithCredentials(new AzureCredentials(tokenCredentials, tokenCredentials, string.Empty, AzureEnvironment.AzureGlobalCloud))
                                   .Build();

            ServiceBusNamespace = Azure.Authenticate(client, string.Empty)
                                       .WithSubscription(ConfigSettings.SubscriptionId)
                                       .ServiceBusNamespaces.List()
                                       .SingleOrDefault(n => n.Name == namespaceName);

            if (ServiceBusNamespace == null)
            {
                throw new InvalidOperationException($"Couldn't find the service bus namespace {namespaceName} in the subscription with ID {ConfigSettings.SubscriptionId}");
            }
        }

        public void Dispose()
        {
        }
    }

    [CollectionDefinition(nameof(AzureServiceBusCollection))]
    public class AzureServiceBusCollection : ICollectionFixture<AzureServiceBusFixture> { }

    /// <summary>
    /// Binder POCO to the keyvault settings:
    ///     --ConnectionString
    ///     --SubscriptionId
    /// </summary>
    public class MessagingSettings
    {
        public string ConnectionString { get; set; }

        public string SubscriptionId { get; set; }
    }
}

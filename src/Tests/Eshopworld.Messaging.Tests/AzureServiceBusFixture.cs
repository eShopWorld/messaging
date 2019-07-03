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

namespace Eshopworld.Messaging.Tests
{
    /// <summary>
    /// To be used on tests that manipulate entities in Azure Service Bus through the fluent SDK.
    ///     Finds and caches the correct instance of <see cref="IServiceBusNamespace"/> to use when
    ///     manipulating entities inside the namespace.
    /// </summary>
    public class AzureServiceBusFixture
    {
        internal const string KeyVaultUriName = "TestKeyvaultUri";
        internal readonly IServiceBusNamespace ServiceBusNamespace;
        internal readonly MessagingSettings ConfigSettings = new MessagingSettings();

        /// <summary>
        /// Initializes a new instance of <see cref="AzureServiceBusFixture"/>.
        /// </summary>
        public AzureServiceBusFixture()
        {
            var keyVaultUri = (Environment.GetEnvironmentVariable(KeyVaultUriName, EnvironmentVariableTarget.Machine) ??
                               Environment.GetEnvironmentVariable(KeyVaultUriName, EnvironmentVariableTarget.User)) ??
                               Environment.GetEnvironmentVariable(KeyVaultUriName, EnvironmentVariableTarget.Process);

            var tokenProvider = new AzureServiceTokenProvider();
            var config = new ConfigurationBuilder().AddAzureKeyVault(
                                                       keyVaultUri,
                                                       new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(tokenProvider.KeyVaultTokenCallback)),
                                                       new DefaultKeyVaultSecretManager())
                                                   .Build();

            config.Bind(ConfigSettings);

            var namespaceName = Regex.Match(ConfigSettings.ServiceBusConnectionString, @"Endpoint=sb:\/\/([^.]*)", RegexOptions.IgnoreCase).Groups[1].Value;

            var token = tokenProvider.GetAccessTokenAsync("https://management.core.windows.net/", string.Empty).Result;
            var tokenCredentials = new TokenCredentials(token);

            var client = RestClient.Configure()
                                   .WithEnvironment(AzureEnvironment.AzureGlobalCloud)
                                   .WithLogLevel(HttpLoggingDelegatingHandler.Level.Basic)
                                   .WithCredentials(new AzureCredentials(tokenCredentials, tokenCredentials, string.Empty, AzureEnvironment.AzureGlobalCloud))
                                   .Build();

            ServiceBusNamespace = Azure.Authenticate(client, string.Empty)
                                       .WithSubscription(ConfigSettings.AzureSubscriptionId)
                                       .ServiceBusNamespaces.List()
                                       .SingleOrDefault(n => n.Name == namespaceName);

            if (ServiceBusNamespace == null)
            {
                throw new InvalidOperationException($"Couldn't find the service bus namespace {namespaceName} in the subscription with ID {ConfigSettings.AzureSubscriptionId}");
            }
        }
    }

    /// <summary>
    /// Collection object that the tests will reference by nameof()
    /// </summary>
    [CollectionDefinition(nameof(AzureServiceBusCollection))]
    public class AzureServiceBusCollection : ICollectionFixture<AzureServiceBusFixture> { }

    /// <summary>
    /// Binder POCO for the <see cref="ConfigurationBuilder"/> to the KeyVault settings:
    ///     --ConnectionString
    ///     --SubscriptionId
    /// </summary>
    public class MessagingSettings
    {
        public string ServiceBusConnectionString { get; set; }

        public string AzureSubscriptionId { get; set; }
    }
}

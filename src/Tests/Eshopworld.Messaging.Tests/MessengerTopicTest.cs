using System.Threading.Tasks;
using Eshopworld.Core;
using Eshopworld.Messaging;
using Eshopworld.Messaging.Tests;
using Eshopworld.Tests.Core;
using Xunit;

// ReSharper disable once CheckNamespace
// ReSharper disable AccessToDisposedClosure
[Collection(nameof(AzureServiceBusCollection))]
public class MessengerTopicTest
{
    internal readonly AzureServiceBusFixture ServiceBusFixture;

    public MessengerTopicTest(AzureServiceBusFixture serviceBusFixture)
    {
        ServiceBusFixture = serviceBusFixture;
    }

    [Fact, IsIntegration]
    public async Task Test_SendCreatesTheTopic()
    {
        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();

        using (IDoFullMessaging msn = new Messenger(ServiceBusFixture.ConfigSettings.ConnectionString, ServiceBusFixture.ConfigSettings.SubscriptionId))
        {
            await msn.Publish(new TestMessage());
            ServiceBusFixture.ServiceBusNamespace.AssertSingleTopicExists(typeof(TestMessage));
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Eshopworld.Core;
using Eshopworld.Messaging;
using Eshopworld.Messaging.Tests;
using Eshopworld.Tests.Core;
using FluentAssertions;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
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

    [Fact, IsIntegration]
    public async Task Test_ReceiveCreatesTheTopic()
    {
        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();

        using (IDoFullMessaging msn = new Messenger(ServiceBusFixture.ConfigSettings.ConnectionString, ServiceBusFixture.ConfigSettings.SubscriptionId))
        {
            var subscriptionName = nameof(Test_ReceiveCreatesTheTopic).Replace("_", "");
            await msn.Subscribe<TestMessage>(_ => { }, subscriptionName);
            ServiceBusFixture.ServiceBusNamespace.AssertSingleTopicSubscriptionExists(typeof(TestMessage), subscriptionName);
        }
    }

    [Fact, IsIntegration]
    public async Task Test_SendingRandomMessages()
    {
        var sendCount = new Random().Next(1, 10);
        var subscriptionName = nameof(Test_SendingRandomMessages).Replace("_", "");

        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();

        var messages = new List<TestMessage>();
        for (var i = 0; i < sendCount; i++) { messages.Add(new TestMessage()); }

        using (IDoFullMessaging msn = new Messenger(ServiceBusFixture.ConfigSettings.ConnectionString, ServiceBusFixture.ConfigSettings.SubscriptionId))
        {
            await msn.Publish(new TestMessage()); // send one to create the topic
            await (await ServiceBusFixture.ServiceBusNamespace.Topics.ListAsync()).First(t => t.Name == typeof(TestMessage).GetEntityName())
                                                                                  .CreateSubscriptionIfNotExists(subscriptionName);

            foreach (var message in messages)
            {
                await msn.Publish(message);
            }

            await Task.Delay(TimeSpan.FromSeconds(5)); // wait 5 seconds to flush out all the messages

            var receiver = new MessageReceiver(ServiceBusFixture.ConfigSettings.ConnectionString, EntityNameHelper.FormatSubscriptionPath(typeof(TestMessage).GetEntityName(), subscriptionName), ReceiveMode.ReceiveAndDelete, null, sendCount);
            var rMessages = (await receiver.ReadBatchAsync<TestMessage>(sendCount))?.ToList();

            rMessages.Should().NotBeNullOrEmpty();
            rMessages.Should().BeEquivalentTo(messages);
        }
    }
}
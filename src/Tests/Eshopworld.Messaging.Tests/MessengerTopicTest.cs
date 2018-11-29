using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Subjects;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Eshopworld.Core;
using Eshopworld.Messaging;
using Eshopworld.Messaging.Tests;
using Eshopworld.Tests.Core;
using FluentAssertions;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Newtonsoft.Json;
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
    public async Task Test_SendCreatesTheTopicAsChildEntityName()
    {
        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();

        using (IDoFullMessaging msn = new Messenger(ServiceBusFixture.ConfigSettings.ConnectionString, ServiceBusFixture.ConfigSettings.SubscriptionId))
        {
            await msn.Publish(new PlatformOrderCreateDomainEvent());
            ServiceBusFixture.ServiceBusNamespace.AssertSingleTopicExists(typeof(PlatformOrderCreateDomainEvent));
        }
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
    public async Task Test_SendingRandomEvents()
    {
        var sendCount = new Random().Next(1, 10);
        var subscriptionName = nameof(Test_SendingRandomEvents).Replace("_", "");

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

    [Fact, IsIntegration]
    public async Task Test_ReceivingRandomEvents()
    {
        var receiveCount = new Random().Next(1, 10);
        var subscriptionName = nameof(Test_ReceivingRandomEvents).Replace("_", "");

        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();

        var rMessages = new List<TestMessage>();
        var messages = new List<TestMessage>();
        for (var i = 0; i < receiveCount; i++) { messages.Add(new TestMessage()); }

        using (var ts = new CancellationTokenSource())
        using (IDoFullMessaging msn = new Messenger(ServiceBusFixture.ConfigSettings.ConnectionString, ServiceBusFixture.ConfigSettings.SubscriptionId))
        {
            // We need to create the messenger before sending the messages to avoid writing non necessary code to create the queue
            // during the test. Receive will create the queue automatically. This breaks the AAA pattern by design.
            // This test flow also ensures that receive will actually create the queue properly.
            await msn.Subscribe<TestMessage>(
                m =>
                {
                    rMessages.Add(m);
                    msn.Complete(m);
                    if (rMessages.Count == messages.Count) ts.Cancel(); // kill switch
                }, subscriptionName);

            var sender = new MessageSender(ServiceBusFixture.ConfigSettings.ConnectionString, typeof(TestMessage).GetEntityName());
            await sender.WriteBatchAsync(messages);

            try
            {
                await Task.Delay(TimeSpan.FromMinutes(5), ts.Token);
            }
            catch (TaskCanceledException) { /* soak the kill switch */ }

            rMessages.Should().BeEquivalentTo(messages);
        }
    }

    [Fact, IsIntegration]
    public async Task Test_ReceivingRandomStringEvents()
    {
        var receiveCount = new Random().Next(1, 10);
        var subscriptionName = nameof(Test_ReceivingRandomEvents).Replace("_", "");

        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();

        var rMessages = new List<TestMessage>();
        var messages = new List<TestMessage>();
        for (var i = 0; i < receiveCount; i++) { messages.Add(new TestMessage()); }

        using (var ts = new CancellationTokenSource())
        using (var messagesSubject = new Subject<Message>())
        using (var adapter = new TopicAdapter<Message>(ServiceBusFixture.ConfigSettings.ConnectionString, ServiceBusFixture.ConfigSettings.SubscriptionId, messagesSubject, 10, typeof(TestMessage)))
        using (messagesSubject.Subscribe(
            m =>
            {
                rMessages.Add(JsonConvert.DeserializeObject<TestMessage>(Encoding.UTF8.GetString(m.Body)));
                adapter.Complete(m).Wait(ts.Token);
                if (rMessages.Count == messages.Count) ts.Cancel(); // kill switch
            }))
        {
            await adapter.StartReading(subscriptionName);

            var sender = new MessageSender(ServiceBusFixture.ConfigSettings.ConnectionString, typeof(TestMessage).GetEntityName());
            await sender.WriteBatchAsync(messages);

            try
            {
                await Task.Delay(TimeSpan.FromMinutes(5), ts.Token);
            }
            catch (TaskCanceledException) { /* soak the kill switch */ }

            rMessages.Should().BeEquivalentTo(messages);
        }
    }
}
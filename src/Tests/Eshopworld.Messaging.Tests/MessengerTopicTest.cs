using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Eshopworld.Core;
using Eshopworld.Messaging;
using Eshopworld.Messaging.Tests;
using Eshopworld.Tests.Core;
using FluentAssertions;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Moq;
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

    [Fact, IsLayer1]
    public async Task Test_SendCreatesTheTopic()
    {
        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();

        using (IDoFullMessaging msn = new Messenger(ServiceBusFixture.ConfigSettings.ServiceBusConnectionString, ServiceBusFixture.ConfigSettings.AzureSubscriptionId))
        {
            await msn.Publish(new TestMessage());
            ServiceBusFixture.ServiceBusNamespace.AssertSingleTopicExists(typeof(TestMessage));
        }
    }

    [Fact, IsLayer1]
    public async Task Test_SendWithTopic_CreatesTheTopic()
    {
        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();
        var topicName = nameof(Test_SendWithTopic_CreatesTheTopic).Replace("_", "");

        using (IDoFullMessaging msn = new Messenger(ServiceBusFixture.ConfigSettings.ServiceBusConnectionString, ServiceBusFixture.ConfigSettings.AzureSubscriptionId))
        {
            await msn.Publish(new TestMessage(), topicName);
            ServiceBusFixture.ServiceBusNamespace.AssertSingleTopicExists(topicName);
        }
    }

    [Fact, IsLayer1]
    public async Task Test_SendFailureCorrectRebuildSequence()
    {
        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();

        using (IDoFullMessaging msn = new Messenger(ServiceBusFixture.ConfigSettings.ServiceBusConnectionString, ServiceBusFixture.ConfigSettings.AzureSubscriptionId))
        {
            await msn.Publish(new TestMessage());
            ServiceBusFixture.ServiceBusNamespace.AssertSingleTopicExists(typeof(TestMessage));
        }

        using (IDoFullMessaging msn = new Messenger(ServiceBusFixture.ConfigSettings.ListenOnlyServiceBusConnectionString, ServiceBusFixture.ConfigSettings.AzureSubscriptionId))
        {
            var tasks = new List<Task>();
            for (int x=0; x<100; x++)
            {
                tasks.Add(Task.Run(()=> msn.Publish(new TestMessage())));
            }

            Task global=null; 
            try
            {
                global = Task.WhenAll(tasks);
                await global;
            }
            catch (Exception)
            {
                Assert.All(global.Exception.InnerExceptions, (t) => Assert.True(t is UnauthorizedException));
            }

        }
    }

    [Fact, IsLayer1]
    public async Task Test_ReceiveCreatesTheTopic()
    {
        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();

        using (IDoFullMessaging msn = new Messenger(ServiceBusFixture.ConfigSettings.ServiceBusConnectionString, ServiceBusFixture.ConfigSettings.AzureSubscriptionId))
        {
            var subscriptionName = nameof(Test_ReceiveCreatesTheTopic).Replace("_", "");
            await msn.Subscribe<TestMessage>(_ => { }, subscriptionName);
            msn.CancelReceive<TestMessage>();
            ServiceBusFixture.ServiceBusNamespace.AssertSingleTopicSubscriptionExists(typeof(TestMessage), subscriptionName);
        }
    }

    [Fact, IsLayer1]
    public async Task Test_ReceiveWithTopicName_CreatesTheTopic()
    {
        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();
        var topicName = nameof(Test_ReceiveWithTopicName_CreatesTheTopic).Replace("_", "");
        topicName = $"{topicName}_Topic";

        using (IDoFullMessaging msn = new Messenger(ServiceBusFixture.ConfigSettings.ServiceBusConnectionString, ServiceBusFixture.ConfigSettings.AzureSubscriptionId))
        {
            var subscriptionName = nameof(Test_ReceiveWithTopicName_CreatesTheTopic).Replace("_", "");
            await msn.Subscribe<TestMessage>(_ => { }, subscriptionName, topicName);
            msn.CancelReceive<TestMessage>(topicName);
            ServiceBusFixture.ServiceBusNamespace.AssertSingleTopicSubscriptionExists(topicName, subscriptionName);
        }
    }

    [Theory, IsLayer1]
    [InlineData(-2)]
    [InlineData(0)]
    [InlineData(4)]
    public async Task Test_ReceiveWithTopicNameAndIdleTime_WhenLessThan5Min_ThrowsException(int minutes)
    {
        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();
        
        var topicName = nameof(Test_ReceiveWithTopicNameAndIdleTime_WhenLessThan5Min_ThrowsException).Replace("_", "");

        using (IDoFullMessaging msn = new Messenger(ServiceBusFixture.ConfigSettings.ServiceBusConnectionString, ServiceBusFixture.ConfigSettings.AzureSubscriptionId))
        {
            var subscriptionName = $"IdleTopicSubscription{minutes}";
            Func<Task> action = async () => await msn.Subscribe<TestMessage>(_ => { }, subscriptionName, topicName,
                deleteOnIdleDurationInMinutes: minutes);
            action.Should().Throw<ArgumentException>().Which.ParamName.Should()
                .BeEquivalentTo("deleteOnIdleDurationInMinutes");
        }
    }

    [Fact, IsLayer1]
    public async Task Test_ReceiveWithTopicNameAndIdleTime_CreatesTheTopicAndRemovesAfterIdle()
    {
        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();
        
        const string topicName = "IdleTopicTest";
        const int idleTimeMin = 5;

        using (IDoFullMessaging msn = new Messenger(ServiceBusFixture.ConfigSettings.ServiceBusConnectionString, ServiceBusFixture.ConfigSettings.AzureSubscriptionId))
        {
            const string subscriptionName = "IdleTopicSubscription";
            await msn.Subscribe<TestMessage>(_ => { }, subscriptionName, topicName, deleteOnIdleDurationInMinutes: idleTimeMin);
            msn.CancelReceive<TestMessage>(topicName);
            ServiceBusFixture.ServiceBusNamespace.AssertSingleTopicSubscriptionExists(topicName, subscriptionName);
            await Task.Delay(TimeSpan.FromMinutes(6));
            ServiceBusFixture.ServiceBusNamespace.AssertSubscriptionDoNotExists(topicName, subscriptionName);
        }
    }

    [Theory, IsLayer1]
    [InlineData(typeof(TestEventWithoutAttribute))]
    [InlineData(typeof(TestEventWithAttribute))]
    [InlineData(typeof(TestEventWithEmptyAttributeValue))]
    [InlineData(typeof(TestEvenWithWhiteSpaceAttributeValue))]
    public async Task Test_SendingNamedEvents(Type type)
    {
        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();

        using (IDoFullMessaging msn = new Messenger(ServiceBusFixture.ConfigSettings.ServiceBusConnectionString, ServiceBusFixture.ConfigSettings.AzureSubscriptionId))
        {
            dynamic message = Activator.CreateInstance(type);
            await msn.Publish(message);
            ServiceBusFixture.ServiceBusNamespace.AssertSingleTopicExists(type);
        }
    }

    [Fact, IsLayer1]
    public async Task Test_SendingRandomEvents()
    {
        var sendCount = new Random().Next(1, 10);
        var subscriptionName = nameof(Test_SendingRandomEvents).Replace("_", "");

        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();

        var messages = new List<TestMessage>();
        for (var i = 0; i < sendCount; i++) { messages.Add(new TestMessage()); }

        using (IDoFullMessaging msn = new Messenger(ServiceBusFixture.ConfigSettings.ServiceBusConnectionString, ServiceBusFixture.ConfigSettings.AzureSubscriptionId))
        {
            await msn.Publish(new TestMessage()); // send one to create the topic
            await (await ServiceBusFixture.ServiceBusNamespace.Topics.ListAsync()).First(t => t.Name == typeof(TestMessage).GetEntityName())
                                                                                  .CreateSubscriptionIfNotExists(subscriptionName);

            foreach (var message in messages)
            {
                await msn.Publish(message);
            }

            await Task.Delay(TimeSpan.FromSeconds(5)); // wait 5 seconds to flush out all the messages

            var receiver = new MessageReceiver(ServiceBusFixture.ConfigSettings.ServiceBusConnectionString, EntityNameHelper.FormatSubscriptionPath(typeof(TestMessage).GetEntityName(), subscriptionName), ReceiveMode.ReceiveAndDelete, null, sendCount);
            var rMessages = (await receiver.ReadBatchAsync<TestMessage>(sendCount))?.ToList();

            rMessages.Should().NotBeNullOrEmpty();
            rMessages.Should().BeEquivalentTo(messages);
        }
    }

    [Fact, IsLayer1]
    public async Task Test_ReceivingRandomEvents()
    {
        var receiveCount = new Random().Next(1, 10);
        var subscriptionName = nameof(Test_ReceivingRandomEvents).Replace("_", "");

        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();

        var rMessages = new List<TestMessage>();
        var messages = new List<TestMessage>();
        for (var i = 0; i < receiveCount; i++) { messages.Add(new TestMessage()); }

        using (var ts = new CancellationTokenSource())
        using (IDoFullMessaging msn = new Messenger(ServiceBusFixture.ConfigSettings.ServiceBusConnectionString, ServiceBusFixture.ConfigSettings.AzureSubscriptionId))
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

            var sender = new MessageSender(ServiceBusFixture.ConfigSettings.ServiceBusConnectionString, typeof(TestMessage).GetEntityName());
            await sender.WriteBatchAsync(messages);

            try
            {
                await Task.Delay(TimeSpan.FromMinutes(5), ts.Token);
            }
            catch (TaskCanceledException) { /* soak the kill switch */ }

            rMessages.Should().BeEquivalentTo(messages);
        }
    }


    /// <summary>Verify the GetTopicByNameAsync extension returns ITopic when exists and null when it does not.</summary>
    [Fact, IsLayer1]
    public async Task Test_GetTopicByName()
    {
        // Arrange
        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();
        var topicName = nameof(Test_GetTopicByName).Replace("_", "");

        // Act
        var topicBefore = await ServiceBusFixture.ServiceBusNamespace.GetTopicByNameAsync(topicName);
        await ServiceBusFixture.ServiceBusNamespace.CreateTopicIfNotExists(topicName);
        var topicAfter = await ServiceBusFixture.ServiceBusNamespace.GetTopicByNameAsync(topicName);

        // Assert
        topicBefore.Should().BeNull();
        topicAfter.Should().NotBeNull();
        topicAfter.Name.Should().Be(topicAfter.Name.ToLowerInvariant());
    }

    /// <summary>Verify the GetTopicSubscriptionByName extension returns ISubscription when exists and null when it does not.</summary>
    [Fact, IsLayer1]
    public async Task Test_GetTopicSubscriptionByName()
    {
        // Arrange
        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();
        var topicName = nameof(Test_GetTopicSubscriptionByName).Replace("_", "");
        var subscriptionName = "Sub1";

        // Act
        var topic = await ServiceBusFixture.ServiceBusNamespace.CreateTopicIfNotExists(topicName);
        var subscriptionBefore = await topic.GetSubscriptionByNameAsync(subscriptionName);
        await topic.CreateSubscriptionIfNotExists(subscriptionName);
        var subscriptionAfter = await topic.GetSubscriptionByNameAsync(subscriptionName);

        // Assert
        subscriptionBefore.Should().BeNull();
        subscriptionAfter.Should().NotBeNull();
        subscriptionAfter.Name.Should().Be(subscriptionName.ToLowerInvariant());
    }
}

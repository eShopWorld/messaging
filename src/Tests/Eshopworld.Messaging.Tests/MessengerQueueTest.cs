using Eshopworld.Core;
using Eshopworld.Messaging;
using Eshopworld.Messaging.Tests;
using Eshopworld.Tests.Core;
using FluentAssertions;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

// ReSharper disable once CheckNamespace
// ReSharper disable AccessToDisposedClosure
[Collection(nameof(AzureServiceBusCollection))]
public class MessengerQueueTest
{
    internal readonly AzureServiceBusFixture ServiceBusFixture;

    public MessengerQueueTest(AzureServiceBusFixture serviceBusFixture)
    {
        ServiceBusFixture = serviceBusFixture;
    }

    [Fact, IsLayer1]
    public async Task Test_SendCreatesTheQueue()
    {
        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();

        using (IDoFullMessaging msn = new Messenger(ServiceBusFixture.ConfigSettings.ServiceBusConnectionString, ServiceBusFixture.ConfigSettings.AzureSubscriptionId))
        {
            await msn.Send(new TestMessage());
            ServiceBusFixture.ServiceBusNamespace.AssertSingleQueueExists(typeof(TestMessage));
        }
    }

    [Fact, IsLayer1]
    public async Task Test_ReceiveCreatesTheQueue()
    {
        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();

        using (IDoFullMessaging msn = new Messenger(ServiceBusFixture.ConfigSettings.ServiceBusConnectionString, ServiceBusFixture.ConfigSettings.AzureSubscriptionId))
        {
            msn.Receive<TestMessage>(_ => { });
            ServiceBusFixture.ServiceBusNamespace.AssertSingleQueueExists(typeof(TestMessage));
        }
    }

    [Fact, IsLayer1]
    public async Task Test_SendingRandomMessages()
    {
        var sendCount = new Random().Next(1, 10);

        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();

        var messages = new List<TestMessage>();
        for (var i = 0; i < sendCount; i++) { messages.Add(new TestMessage()); }

        using (IDoFullMessaging msn = new Messenger(ServiceBusFixture.ConfigSettings.ServiceBusConnectionString, ServiceBusFixture.ConfigSettings.AzureSubscriptionId))
        {
            foreach (var message in messages)
            {
                await msn.Send(message);
            }

            await Task.Delay(TimeSpan.FromSeconds(5)); // wait 5 seconds to flush out all the messages

            var receiver = new MessageReceiver(ServiceBusFixture.ConfigSettings.ServiceBusConnectionString, typeof(TestMessage).GetEntityName(), ReceiveMode.ReceiveAndDelete, null, sendCount);
            var rMessages = (await receiver.ReadBatchAsync<TestMessage>(sendCount)).ToList();

            rMessages.Should().BeEquivalentTo(messages);
        }
    }

    [Fact, IsLayer1]
    public async Task Test_ReceivingRandomMessages()
    {
        var receiveCount = new Random().Next(1, 10);
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
            msn.Receive<TestMessage>(
                m =>
                {
                    rMessages.Add(m);
                    msn.Complete(m);
                    if (rMessages.Count == messages.Count) ts.Cancel(); // kill switch
                });

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

    [Fact, IsLayer1]
    public async Task Test_LockMessage_ForTwoMinutes()
    {
        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();

        using (IDoFullMessaging msn = new Messenger(ServiceBusFixture.ConfigSettings.ServiceBusConnectionString, ServiceBusFixture.ConfigSettings.AzureSubscriptionId))
        {
            await msn.Send(new TestMessage());

            var message = default(TestMessage);
            msn.Receive<TestMessage>(
                async m =>
                {
                    message = m;
                    await msn.Lock(m);
                });

            await Task.Delay(TimeSpan.FromMinutes(2));

            message.Should().NotBeNull();
            await msn.Complete(message); // If this throws, Lock failed
        }
    }

    [Fact, IsLayer1]
    public async Task Test_LockAbandon_MessageFlow()
    {
        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();

        using (var ts = new CancellationTokenSource())
        using (var msn = new Messenger(ServiceBusFixture.ConfigSettings.ServiceBusConnectionString, ServiceBusFixture.ConfigSettings.AzureSubscriptionId))
        {
            await msn.Send(new TestMessage());

            msn.Receive<TestMessage>(
                async m =>
                {
                    await msn.Lock(m);
                    msn.CancelReceive<TestMessage>();
                    await Task.Delay(TimeSpan.FromSeconds(5), ts.Token);
                    await msn.Abandon(m);

                    ts.Cancel(); // kill switch
                });

            try
            {
                await Task.Delay(TimeSpan.FromMinutes(2), ts.Token);
            }
            catch (TaskCanceledException) { /* soak the kill switch */ }

            ((QueueAdapter<TestMessage>)msn.ServiceBusAdapters[typeof(TestMessage).FullName]).Messages.Should().BeEmpty();
            ((QueueAdapter<TestMessage>)msn.ServiceBusAdapters[typeof(TestMessage).FullName]).LockTimers.Should().BeEmpty();
        }
    }

    [Fact, IsLayer1]
    public async Task Test_LockComplete_MessageFlow()
    {
        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();

        using (var ts = new CancellationTokenSource())
        using (var msn = new Messenger(ServiceBusFixture.ConfigSettings.ServiceBusConnectionString, ServiceBusFixture.ConfigSettings.AzureSubscriptionId))
        {
            await msn.Send(new TestMessage());

            msn.Receive<TestMessage>(
                async m =>
                {
                    await msn.Lock(m);
                    await Task.Delay(TimeSpan.FromSeconds(5), ts.Token);
                    await msn.Complete(m);

                    ts.Cancel(); // kill switch
                });

            try
            {
                await Task.Delay(TimeSpan.FromMinutes(2), ts.Token);
            }
            catch (TaskCanceledException) { /* soak the kill switch */ }

            ((QueueAdapter<TestMessage>)msn.ServiceBusAdapters[typeof(TestMessage).FullName]).Messages.Should().BeEmpty();
            ((QueueAdapter<TestMessage>)msn.ServiceBusAdapters[typeof(TestMessage).FullName]).LockTimers.Should().BeEmpty();
        }
    }

    [Fact, IsLayer1]
    public async Task Test_LockError_MessageFlow()
    {
        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();

        using (var ts = new CancellationTokenSource())
        using (var msn = new Messenger(ServiceBusFixture.ConfigSettings.ServiceBusConnectionString, ServiceBusFixture.ConfigSettings.AzureSubscriptionId))
        {
            var message = new TestMessage();
            await msn.Send(message);

            msn.Receive<TestMessage>(
                async m =>
                {
                    await msn.Lock(m);
                    await Task.Delay(TimeSpan.FromSeconds(5), ts.Token);
                    await msn.Error(m);

                    ts.Cancel(); // kill switch
                });

            try
            {
                await Task.Delay(TimeSpan.FromMinutes(2), ts.Token);
            }
            catch (TaskCanceledException) { /* soak the kill switch */ }

            var receiver = new MessageReceiver(ServiceBusFixture.ConfigSettings.ServiceBusConnectionString, EntityNameHelper.FormatDeadLetterPath(typeof(TestMessage).GetEntityName()), ReceiveMode.ReceiveAndDelete);
            var rMessage = (await receiver.ReadBatchAsync<TestMessage>(1)).FirstOrDefault();

            rMessage.Should().NotBeNull();
            rMessage.Should().BeEquivalentTo(message);

            ((QueueAdapter<TestMessage>)msn.ServiceBusAdapters[typeof(TestMessage).FullName]).Messages.Should().BeEmpty();
            ((QueueAdapter<TestMessage>)msn.ServiceBusAdapters[typeof(TestMessage).FullName]).LockTimers.Should().BeEmpty();
        }
    }

    /// <summary>Verify the GetQueueByNameAsync extension returns IQueue when exists and null when it does not.</summary>
    [Fact, IsLayer1]
    public async Task Test_GetQueueByName()
    {
        // Arrange
        await ServiceBusFixture.ServiceBusNamespace.ScorchNamespace();
        var queueName = nameof(Test_GetQueueByName).Replace("_", "");

        // Act
        var queueBefore = await ServiceBusFixture.ServiceBusNamespace.GetTopicByNameAsync(queueName);
        await ServiceBusFixture.ServiceBusNamespace.CreateQueueIfNotExists(queueName);
        var queueAfter = await ServiceBusFixture.ServiceBusNamespace.GetTopicByNameAsync(queueName);

        // Assert
        queueBefore.Should().BeNull();
        queueAfter.Should().NotBeNull();
        queueAfter.Name.Should().Be(queueName.ToLowerInvariant());
    }

}
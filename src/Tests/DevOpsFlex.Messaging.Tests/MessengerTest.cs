using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DevOpsFlex.Messaging;
using DevOpsFlex.Messaging.Tests;
using FluentAssertions;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Xunit;

// ReSharper disable once CheckNamespace
// ReSharper disable AccessToDisposedClosure
public class MessengerTest
{
    [Collection("Integration")]
    public class Integration
    {
        [Theory, Trait("Category", "Integration")]
        [MemberData(nameof(GetData_TestMessageTypes))]
        public async Task Test_SendCreatesTheQueue<T>(T _)
            where T : IMessage, new() // be careful with this, if the test doesn't run it's because the T validation is broken
        {
            _.CheckInlineType(); // inline data check

            var nsm = NamespaceManager.CreateFromConnectionString(NamespaceHelper.GetConnectionString());
            await nsm.ScorchNamespace();
            MessageQueue.LockTimers.Release();
            MessageQueue.BrokeredMessages.Release();

            using (IMessenger msn = new Messenger(NamespaceHelper.GetConnectionString()))
            {
                await msn.Send(new T());

                await Task.Delay(TimeSpan.FromSeconds(5)); // wait 5 seconds to flush out all the messages

                (await nsm.GetQueuesAsync()).ToList().AssertSingleQueueExists(typeof(T));
            }
        }

        [Theory, Trait("Category", "Integration")]
        [MemberData(nameof(GetData_TestMessageTypes))]
        public async Task Test_ReceiveCreatesTheQueue<T>(T _)
            where T : IMessage, new() // be careful with this, if the test doesn't run it's because the T validation is broken
        {
            _.CheckInlineType(); // inline data check

            var nsm = NamespaceManager.CreateFromConnectionString(NamespaceHelper.GetConnectionString());
            await nsm.ScorchNamespace();
            MessageQueue.LockTimers.Release();
            MessageQueue.BrokeredMessages.Release();

            using (IMessenger msn = new Messenger(NamespaceHelper.GetConnectionString()))
            {
                msn.Receive<T>(__ => { });
                (await nsm.GetQueuesAsync()).ToList().AssertSingleQueueExists(typeof(T));
            }
        }

        [Theory, Trait("Category", "Integration")]
        [MemberData(nameof(GetData_TestMessageTypes))]
        public async Task Test_SendingRandomMessages<T>(T _)
            where T : IMessage, new() // be careful with this, if the test doesn't run it's because the T validation is broken
        {
            _.CheckInlineType(); // inline data check

            var sendCount = new Random().Next(1, 10);
            await NamespaceManager.CreateFromConnectionString(NamespaceHelper.GetConnectionString()).ScorchNamespace();
            MessageQueue.LockTimers.Release();
            MessageQueue.BrokeredMessages.Release();

            var messages = new List<T>();
            for (var i = 0; i < sendCount; i++) { messages.Add(new T()); }

            using (IMessenger msn = new Messenger(NamespaceHelper.GetConnectionString()))
            {
                foreach (var message in messages)
                {
                    await msn.Send(message);
                }

                await Task.Delay(TimeSpan.FromSeconds(5)); // wait 5 seconds to flush out all the messages

                var qClient = QueueClient.CreateFromConnectionString(NamespaceHelper.GetConnectionString(), typeof(T).GetQueueName());
                var rMessages = (await qClient.ReadBatchAsync<T>(sendCount)).ToList();
                rMessages.Should().BeEquivalentTo(messages);

                qClient.Close();
            }
        }

        [Theory, Trait("Category", "Integration")]
        [MemberData(nameof(GetData_TestMessageTypes))]
        public async Task Test_ReceivingRandomMessages<T>(T _)
            where T : IMessage, new() // be careful with this, if the test doesn't run it's because the T validation is broken
        {
            _.CheckInlineType(); // inline data check

            var receiveCount = new Random().Next(1, 10);
            await NamespaceManager.CreateFromConnectionString(NamespaceHelper.GetConnectionString()).ScorchNamespace();
            MessageQueue.LockTimers.Release();
            MessageQueue.BrokeredMessages.Release();

            var rMessages = new List<T>();
            var messages = new List<T>();
            for (var i = 0; i < receiveCount; i++) { messages.Add(new T()); }

            using (var ts = new CancellationTokenSource())
            using (IMessenger msn = new Messenger(NamespaceHelper.GetConnectionString()))
            {
                // We need to create the messenger before sending the messages to avoid writing unecessary code to create the queue
                // during the test. Receive will create the queue automatically. This breaks the AAA pattern by design.
                // This test flow also ensures that receive will actually create the queue properly.
                msn.Receive<T>(
                    m =>
                    {
                        rMessages.Add(m);
                        if (rMessages.Count == messages.Count) ts.Cancel(); // kill switch
                    });

                var qClient = QueueClient.CreateFromConnectionString(NamespaceHelper.GetConnectionString(), typeof(T).GetQueueName());
                await qClient.WriteBatchAsync(messages);

                try
                {
                    await Task.Delay(TimeSpan.FromMinutes(2), ts.Token);
                }
                catch (TaskCanceledException) { /* soak the kill switch */ }

                rMessages.Should().BeEquivalentTo(messages);
            }
        }

        [Theory, Trait("Category", "Integration")]
        [MemberData(nameof(GetData_TestMessageTypes))]
        public async Task Test_LockMessage_ForFiveMinutes<T>(T _)
            where T : IMessage, new() // be careful with this, if the test doesn't run it's because the T validation is broken
        {
            _.CheckInlineType(); // inline data check

            await NamespaceManager.CreateFromConnectionString(NamespaceHelper.GetConnectionString()).ScorchNamespace();
            MessageQueue.LockTimers.Release();
            MessageQueue.BrokeredMessages.Release();

            using (IMessenger msn = new Messenger(NamespaceHelper.GetConnectionString()))
            {
                await msn.Send(new T());

                var message = default(T);
                msn.Receive<T>(
                    async m =>
                    {
                        message = m;
                        await message.Lock();
                    });

                await Task.Delay(TimeSpan.FromMinutes(5));

                message.Should().NotBeNull();
                await message.Complete(); // If this throws, Lock failed
            }
        }

        [Theory, Trait("Category", "Integration")]
        [MemberData(nameof(GetData_TestMessageTypes))]
        public async Task Test_LockAbandon_MessageFlow<T>(T _)
            where T : IMessage, new() // be careful with this, if the test doesn't run it's because the T validation is broken
        {
            _.CheckInlineType(); // inline data check

            await NamespaceManager.CreateFromConnectionString(NamespaceHelper.GetConnectionString()).ScorchNamespace();
            MessageQueue.LockTimers.Release();
            MessageQueue.BrokeredMessages.Release();

            using (var ts = new CancellationTokenSource())
            using (IMessenger msn = new Messenger(NamespaceHelper.GetConnectionString()))
            {
                await msn.Send(new T());

                msn.Receive<T>(
                    async m =>
                    {
                        await m.Lock();
                        msn.CancelReceive<T>();
                        await Task.Delay(TimeSpan.FromSeconds(5), ts.Token);
                        await m.Abandon();

                        ts.Cancel(); // kill switch
                    });

                try
                {
                    await Task.Delay(TimeSpan.FromMinutes(2), ts.Token);
                }
                catch (TaskCanceledException) { /* soak the kill switch */ }

                MessageQueue.BrokeredMessages.Should().BeEmpty();
                MessageQueue.LockTimers.Should().BeEmpty();
            }
        }

        [Theory, Trait("Category", "Integration")]
        [MemberData(nameof(GetData_TestMessageTypes))]
        public async Task Test_LockComplete_MessageFlow<T>(T _)
            where T : IMessage, new() // be careful with this, if the test doesn't run it's because the T validation is broken
        {
            _.CheckInlineType(); // inline data check

            await NamespaceManager.CreateFromConnectionString(NamespaceHelper.GetConnectionString()).ScorchNamespace();
            MessageQueue.LockTimers.Release();
            MessageQueue.BrokeredMessages.Release();

            using (var ts = new CancellationTokenSource())
            using (IMessenger msn = new Messenger(NamespaceHelper.GetConnectionString()))
            {
                await msn.Send(new T());

                msn.Receive<T>(
                    async m =>
                    {
                        await m.Lock();
                        await Task.Delay(TimeSpan.FromSeconds(5), ts.Token);
                        await m.Complete();

                        ts.Cancel(); // kill switch
                    });

                try
                {
                    await Task.Delay(TimeSpan.FromMinutes(2), ts.Token);
                }
                catch (TaskCanceledException) { /* soak the kill switch */ }

                MessageQueue.BrokeredMessages.Should().BeEmpty();
                MessageQueue.LockTimers.Should().BeEmpty();
            }
        }

        [Theory, Trait("Category", "Integration")]
        [MemberData(nameof(GetData_TestMessageTypes))]
        public async Task Test_LockError_MessageFlow<T>(T _)
            where T : IMessage, new() // be careful with this, if the test doesn't run it's because the T validation is broken
        {
            _.CheckInlineType(); // inline data check

            await NamespaceManager.CreateFromConnectionString(NamespaceHelper.GetConnectionString()).ScorchNamespace();
            MessageQueue.LockTimers.Release();
            MessageQueue.BrokeredMessages.Release();

            using (var ts = new CancellationTokenSource())
            using (IMessenger msn = new Messenger(NamespaceHelper.GetConnectionString()))
            {
                var message = new T();
                await msn.Send(message);

                msn.Receive<T>(
                    async m =>
                    {
                        await m.Lock();
                        await Task.Delay(TimeSpan.FromSeconds(5), ts.Token);
                        await m.Error();

                        ts.Cancel(); // kill switch
                    });

                try
                {
                    await Task.Delay(TimeSpan.FromMinutes(2), ts.Token);
                }
                catch (TaskCanceledException) { /* soak the kill switch */ }

                var qClient = QueueClient.CreateFromConnectionString(NamespaceHelper.GetConnectionString(), QueueClient.FormatDeadLetterPath(typeof(T).GetQueueName()));
                var rMessage = (await qClient.ReadBatchAsync<T>(1)).First();

                rMessage.ShouldBeEquivalentTo(message);

                MessageQueue.BrokeredMessages.Should().BeEmpty();
                MessageQueue.LockTimers.Should().BeEmpty();
            }
        }

        public static IEnumerable<object[]> GetData_TestMessageTypes()
        {
            yield return new object[] { new TestMessage() };
        }
    }
}

public static class MessengerTestExtensions
{
    public static void CheckInlineType<T>(this T _)
        where T : IMessage, new()
    {
        // inline data check, kill the test with a detailed message if it's not ITestMessage<T>
        typeof(ITestMessage<T>).IsAssignableFrom(typeof(T))
                               .Should()
                               .BeTrue($"You need to inline classes that implement {nameof(ITestMessage<T>)}, which isn't the case for {typeof(T).FullName}");
    }

    public static void AssertSingleQueueExists(this List<QueueDescription> queues, Type type)
    {
        queues.Should().HaveCount(1);
        queues.SingleOrDefault(q => string.Equals(q.Path, type.GetQueueName(), StringComparison.CurrentCultureIgnoreCase)).Should().NotBeNull();
    }
}

namespace DevOpsFlex.Messaging.Tests
{
    /// <summary>
    /// A convenient way to generate random Lorem text <see cref="IMessage"/>.
    /// </summary>
    public class TestMessage : ITestMessage<TestMessage>
    {
        private static readonly Random Rng = new Random();

        public string Name { get; set; }

        public string Stuff { get; set; }

        public float Price { get; set; }

        public TestMessage()
        {
            Name = Lorem.GetSentence();
            Stuff = Lorem.GetParagraph();
            Price = Rng.Next(100);
        }

        public bool Equals(TestMessage other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(Name, other.Name) &&
                   string.Equals(Stuff, other.Stuff) &&
                   Price.Equals(other.Price);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((TestMessage)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Name?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ (Stuff?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ Price.GetHashCode();
                return hashCode;
            }
        }
    }
}

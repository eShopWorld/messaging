using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DevOpsFlex.Messaging;
using DevOpsFlex.Messaging.Tests;
using FluentAssertions;
using JetBrains.Annotations;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Xunit;

// ReSharper disable once CheckNamespace
public class MessengerTest
{
    [Collection("Integration")]
    public class Integration
    {
        [Theory, Trait("Category", "Integration")]
        [MemberData(nameof(GetData_TestMessageTypes))]
        public async Task Test_SendingRandomMessages<T>(T _)
            where T : IMessage, new() // be careful with this, if the test doesn't run it's because the T validation is broken
        {
            // inline data check, kill the test if it's not ITestMessage<T>
            typeof(ITestMessage<T>).IsAssignableFrom(typeof(T))
                                   .Should()
                                   .BeTrue($"You need to inline classes that implement {nameof(ITestMessage<T>)}, which isn't the case for {typeof(T).FullName}");

            var sendCount = new Random().Next(1, 10);
            await NamespaceManager.CreateFromConnectionString(NamespaceHelper.GetConnectionString()).ScorchNamespace();

            var messages = new List<T>();
            for (var i = 0; i < sendCount; i++) { messages.Add(new T()); }

            using (IMessenger msn = new Messenger(NamespaceHelper.GetConnectionString()))
            {
                foreach (var message in messages)
                {
                    msn.Send(message);
                }

                await Task.Delay(TimeSpan.FromSeconds(5)); // wait 5 seconds to flush out all the messages

                var qClient = QueueClient.CreateFromConnectionString(NamespaceHelper.GetConnectionString(), typeof(T).FullName);
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
            // inline data check, kill the test if it's not ITestMessage<T>
            typeof(ITestMessage<T>).IsAssignableFrom(typeof(T))
                                   .Should()
                                   .BeTrue($"You need to inline classes that implement {nameof(ITestMessage<T>)}, which isn't the case for {typeof(T).FullName}");

            var receiveCount = new Random().Next(1, 10);
            await NamespaceManager.CreateFromConnectionString(NamespaceHelper.GetConnectionString()).ScorchNamespace();

            // We need to create the messenger before sending the messages to avoid writing unecessary code to create the queue
            // during the test. Receive will create the queue automatically. This breaks the AAA pattern by design.
            var rMessages = new List<T>();
            var messages = new List<T>();
            for (var i = 0; i < receiveCount; i++) { messages.Add(new T()); }

            // ReSharper disable once AccessToDisposedClosure
            using (var ts = new CancellationTokenSource())
            using (IMessenger msn = new Messenger(NamespaceHelper.GetConnectionString()))
            {
                msn.Receive<T>(m =>
                {
                    rMessages.Add(m);
                    if (rMessages.Count == messages.Count) ts.Cancel(); // kill the await
                });

                var qClient = QueueClient.CreateFromConnectionString(NamespaceHelper.GetConnectionString(), typeof(T).FullName);
                await qClient.WriteBatchAsync(messages);

                try
                {
                    await Task.Delay(TimeSpan.FromMinutes(2), ts.Token);
                }
                catch (TaskCanceledException) { /* soak the kill switch */ }

                rMessages.Should().BeEquivalentTo(messages);
            }
        }

        public static IEnumerable<object[]> GetData_TestMessageTypes()
        {
            yield return new object[] { new TestMessage() };
        }
    }
}

namespace DevOpsFlex.Messaging.Tests
{
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

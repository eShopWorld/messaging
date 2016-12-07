using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using DevOpsFlex.Messaging;
using DevOpsFlex.Messaging.Tests;
using FluentAssertions;
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
        [MemberData(nameof(GetData_Test_SendingRandomMessages))]
        public async Task Test_SendingRandomMessages<T>(T _)
            where T : IMessage, new() // be careful with this, if the test doesn't run it's because the T validation is broken
        {
            // inline data check
            typeof(ITestMessage<T>).IsAssignableFrom(typeof(T)).Should().BeTrue();

            var sendCount = new Random().Next(1, 10);
            await NamespaceManager.CreateFromConnectionString(NamespaceHelper.GetConnectionString()).ScorchNamespace();

            var qClient = QueueClient.CreateFromConnectionString(NamespaceHelper.GetConnectionString(), typeof(T).FullName);
            IMessenger msn = new Messenger(NamespaceHelper.GetConnectionString());
            var messages = Enumerable.Repeat(new T(), sendCount).ToList();

            foreach (var message in messages)
            {
                msn.Send(message);
            }

            var rMessages = (await qClient.ReceiveBatchAsync(sendCount)).ToList();
            messages.Should().HaveSameCount(rMessages);

            foreach (var message in rMessages)
            {
#if DEBUG
                var rMessage = message.GetBody<T>(new DataContractSerializer(typeof(T)));
#else
                var rMessage = message.GetBody<T>();
#endif
                messages.Should().Contain(rMessage);
            }
        }

        public static IEnumerable<object[]> GetData_Test_SendingRandomMessages()
        {
            yield return new object[] {new TestMessage()};
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
            return Equals((TestMessage) obj);
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

using System;
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
        [Fact, Trait("Category", "Integration")]
        public async Task Test_SendingFiveRandomMessages()
        {
            const int sendCount = 5;
            await NamespaceManager.CreateFromConnectionString(NamespaceHelper.GetConnectionString()).ScorchNamespace();

            var qClient = QueueClient.CreateFromConnectionString(NamespaceHelper.GetConnectionString(), typeof(TestMessage).FullName);
            IMessenger msn = new Messenger(NamespaceHelper.GetConnectionString());
            var messages = TestMessage.Random(sendCount).ToList();

            foreach (var message in messages)
            {
                msn.Send(message);
            }

            var rMessages = (await qClient.ReceiveBatchAsync(sendCount)).ToList();
            messages.Should().HaveSameCount(rMessages);

            foreach (var message in rMessages)
            {
#if DEBUG
                var rMessage = message.GetBody<TestMessage>(new DataContractSerializer(typeof(TestMessage)));
#else
                var rMessage = message.GetBody<TestMessage>();
#endif
                messages.Should().Contain(rMessage);
            }
        }
    }
}

namespace DevOpsFlex.Messaging.Tests
{
    using System.Collections.Generic;

    public class TestMessage : IMessage, IEquatable<TestMessage>
    {
        private static readonly Random Rng = new Random();

        public string Name { get; set; }

        public string Stuff { get; set; }

        public float Price { get; set; }

        public static TestMessage Random()
        {
            return new TestMessage
            {
                Name = Lorem.GetSentence(),
                Stuff = Lorem.GetParagraph(),
                Price = Rng.Next(100)
            };
        }

        public static IEnumerable<TestMessage> Random(int count)
        {
            for (var i = 0; i < count; i++)
            {
                yield return new TestMessage
                {
                    Name = Lorem.GetSentence(),
                    Stuff = Lorem.GetParagraph(),
                    Price = Rng.Next(100)
                };
            }
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

using System;
using System.Threading.Tasks;
using DevOpsFlex.Messaging;
using DevOpsFlex.Messaging.Tests;
using Xunit;

// ReSharper disable once CheckNamespace
public class MessengerTest
{
    [Collection("Integration")]
    public class Integration
    {
        [Fact, Trait("Category", "Integration")]
        public void TestMethod1()
        {
            // MAKE SURE WE PERFORM THE INITIAL SCORCH - extend namespace manager?

            IMessenger msn = new Messenger(NamespaceHelper.GetConnectionString());

            // TAP INTO READS OUTSIDE THE MESSENGER - extend queue client?

            Task.Delay(TimeSpan.FromSeconds(30)).Wait();
        }
    }
}

namespace DevOpsFlex.Messaging.Tests
{
    public class TestMessage : IMessage
    {
        public string Name { get; set; }

        public string Stuff { get; set; }

        public float Price { get; set; }
    }
}

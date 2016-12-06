using System;
using System.Threading.Tasks;
using DevOpsFlex.Messaging;
using DevOpsFlex.Messaging.Tests;
using Xunit;

// ReSharper disable once CheckNamespace
public class MessengerTest
{
    [Fact, Trait("Category", "Integration")]
    public void TestMethod1()
    {
        IMessenger msn = new Messenger(NamespaceHelper.GetConnectionString());
        //msn.Send(new NewCatalogEntry
        //{
        //    Name = "David",
        //    Price = 1.0f,
        //});

        msn.Receive<TestMessage>(m =>
        {
            
        });

        Task.Delay(TimeSpan.FromSeconds(30)).Wait();
    }
}

namespace DevOpsFlex.Messaging.Tests
{
    public class TestMessage : IMessage
    {
        public string Name { get; set; }

        public float Price { get; set; }
    }
}

using System;
using System.Configuration;
using System.Reactive.Linq;
using System.Threading.Tasks;
using DevOpsFlex.Messaging;
using Xunit;

// ReSharper disable once CheckNamespace
public class MessengerTest
{
    [Fact]
    public void TestMethod1()
    {
        IMessenger msn = new Messenger(ConfigurationManager.AppSettings["sb:connection"]);
        //msn.Send(new NewCatalogEntry
        //{
        //    Name = "David",
        //    Price = 1.0f,
        //});

        msn.Receive<NewCatalogEntry>(m =>
        {
            
        });

        Task.Delay(TimeSpan.FromSeconds(30)).Wait();
    }
}

public class NewCatalogEntry : IMessage
{
    public string Name { get; set; }

    public float Price { get; set; }
}

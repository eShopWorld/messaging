namespace DevOpsFlex.Messaging.Profiler
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using FluentAssertions;
    using JetBrains.dotMemoryUnit;
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;
    using Microsoft.VisualStudio.Profiler;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Tests;

    [TestClass]
    public class MessengerProfiler
    {
        [TestMethod, TestCategory("CPUProfiler")]
        public async Task CPUProfile_SendBurst_100_TestMessage()
        {
            await SendBurst(100);
        }

        [TestMethod, TestCategory("CPUProfiler")]
        public async Task CPUProfile_SendBurst_1000_TestMessage()
        {
            await SendBurst(1000);
        }

        /// <remarks>
        /// This is a dotMemory test, so it requires R# + dotMemory and it needs to be run
        /// through the R# test runner using the specific "run under dotMemory Unit" option or it will throw.
        /// </remarks>
        [TestMethod, TestCategory("MemoryProfiler")]
        public void MemoryProfile_SendBurst_100_NoMessageLeaks()
        {
            const int count = 100;
            SendBurstMemoryIsolation(count).Wait();
            GC.Collect(2, GCCollectionMode.Forced);

            dotMemory.Check(memory => memory.GetObjects(o => o.Type == typeof(TestMessage)).ObjectsCount.Should().Be(0));

            // can't guarantee full release inside the ASB SDK, just need to ensure that at the end of the test retention is low (below 5%)
            dotMemory.Check(memory => memory.GetObjects(o => o.Type == typeof(BrokeredMessage)).ObjectsCount.Should().BeLessThan((int)(count * 0.05)));
        }

        private static async Task SendBurst(int count)
        {
            DataCollection.StopProfile(ProfileLevel.Global, DataCollection.CurrentId);
            await NamespaceManager.CreateFromConnectionString(NamespaceHelper.GetConnectionString()).ScorchNamespace();

            var messages = new List<TestMessage>();
            for (var i = 0; i < count; i++)
            {
                messages.Add(new TestMessage());
            }

            using (IMessenger msn = new Messenger(NamespaceHelper.GetConnectionString()))
            {
                // send one to get the queues created outside the collection
                await msn.Send(new TestMessage());

                DataCollection.StartProfile(ProfileLevel.Global, DataCollection.CurrentId);
                foreach (var message in messages)
                {
                    await msn.Send(message);
                }
            }
        }

        private static async Task SendBurstMemoryIsolation(int count)
        {
            await NamespaceManager.CreateFromConnectionString(NamespaceHelper.GetConnectionString()).ScorchNamespace();

            var messages = new List<TestMessage>();
            for (var i = 0; i < count; i++)
            {
                messages.Add(new TestMessage());
            }

            using (IMessenger msn = new Messenger(NamespaceHelper.GetConnectionString()))
            {
                DataCollection.StartProfile(ProfileLevel.Global, DataCollection.CurrentId);
                foreach (var message in messages)
                {
                    await msn.Send(message);
                }
            }

            messages.Clear(); // force List release
        }
    }
}

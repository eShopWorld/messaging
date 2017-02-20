using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DevOpsFlex.Messaging;
using DevOpsFlex.Messaging.Tests;
using DevOpsFlex.Tests.Core;
using FluentAssertions;
using JetBrains.dotMemoryUnit;
using JetBrains.Profiler.Windows.Api;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Xunit;
using Xunit.Abstractions;

// ReSharper disable once CheckNamespace
public class MessengerProfiler
{
    private readonly ITestOutputHelper _output;

    public MessengerProfiler(ITestOutputHelper output)
    {
        _output = output;
        DotMemoryUnitTestOutput.SetOutputMethod(s => _output.WriteLine(s));
    }

    /// <remarks>
    /// This is a dotTrace test, so it requires R# + dotTrace and it needs to be run
    /// through the R# test runner using the specific "profile" option.
    /// </remarks>
    [Fact, IsProfilerCpu]
    public async Task CPUProfile_SendBurst_100_TestMessage()
    {
        MessageQueue.LockTimers.Release();
        MessageQueue.BrokeredMessages.Release();

        InitJbProfilers();
        await SendBurst(100);
    }

    /// <remarks>
    /// This is a dotTrace test, so it requires R# + dotTrace and it needs to be run
    /// through the R# test runner using the specific "profile" option.
    /// </remarks>
    [Fact, IsProfilerCpu]
    public async Task CPUProfile_SendBurst_1000_TestMessage()
    {
        MessageQueue.LockTimers.Release();
        MessageQueue.BrokeredMessages.Release();

        InitJbProfilers();
        await SendBurst(1000);
    }

    /// <remarks>
    /// This is a dotMemory test, so it requires R# + dotMemory and it needs to be run
    /// through the R# test runner using the specific "run under dotMemory Unit" option or it will throw.
    /// </remarks>
    [Fact, IsProfilerMemory]
    public void MemoryProfile_SendBurst_100_NoMessageLeaks()
    {
        const int count = 100;
        MessageQueue.LockTimers.Release();
        MessageQueue.BrokeredMessages.Release();

        SendBurstMemoryIsolation(count).Wait();
        GC.Collect(2, GCCollectionMode.Forced);

        dotMemory.Check(memory => memory.GetObjects(o => o.Type == typeof(TestMessage)).ObjectsCount.Should().Be(0));

        // can't guarantee full release inside the ASB SDK, just need to ensure that at the end of the test retention is low (below 5%)
        dotMemory.Check(memory => memory.GetObjects(o => o.Type == typeof(BrokeredMessage)).ObjectsCount.Should().BeLessThan((int)(count * 0.05)));
    }

    private static async Task SendBurst(int count)
    {
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

            StartJbProfilers(); // PROFILER START
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
            foreach (var message in messages)
            {
                await msn.Send(message);
            }
        }

        messages.Clear(); // force List de-refence to improve the GC.Collect
    }

    private static void InitJbProfilers()
    {
        if (PerformanceProfiler.IsActive)
        {
            PerformanceProfiler.Stop();
        }
        else
        {
            PerformanceProfiler.Begin();
        }
    }

    private static void StartJbProfilers()
    {
        if (PerformanceProfiler.IsActive)
        {
            PerformanceProfiler.Start();
        }
    }
}
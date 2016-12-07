namespace DevOpsFlex.Messaging.Tests
{
    using System;

    public interface ITestMessage<T> : IMessage, IEquatable<T>
    {
    }
}

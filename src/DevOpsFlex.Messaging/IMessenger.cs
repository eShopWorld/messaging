namespace DevOpsFlex.Messaging
{
    using System;
    using JetBrains.Annotations;

    public interface IMessenger
    {
        void Send<T>([NotNull]T message)
            where T : IMessage;

        void Receive<T>([NotNull]Action<T> callback)
            where T : IMessage;
    }
}
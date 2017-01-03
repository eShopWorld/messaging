namespace DevOpsFlex.Messaging
{
    using System;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using JetBrains.Annotations;
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;

    /// <summary>
    /// Contains extension methods for <see cref="QueueClient"/>.
    /// </summary>
    public static class QueueCllientExtensions
    {
        /// <summary>
        /// Creates a <see cref="QueueClient"/> for a given queue.
        /// If the queue doesn't exist this will created it before instantiating the <see cref="QueueClient"/>.
        /// </summary>
        /// <param name="connectionString">The service bus connection string.</param>
        /// <param name="entityPath">The entity path for the queue we are connecting to.</param>
        /// <returns>The final <see cref="QueueClient"/>.</returns>
        [NotNull]internal static async Task<QueueClient> CreateIfNotExists([NotNull]string connectionString, [NotNull]string entityPath)
        {
            var nsm = NamespaceManager.CreateFromConnectionString(connectionString);

            if (!await nsm.QueueExistsAsync(entityPath).ConfigureAwait(false))
            {
                await nsm.CreateQueueAsync(entityPath); // TODO: MISSING QUEUE CREATION PROPERTIES
            }

            return QueueClient.CreateFromConnectionString(connectionString, entityPath, ReceiveMode.PeekLock);
        }
    }

    /// <summary>
    /// Contains extensions methods that are <see cref="QueueClient"/> related but extend
    /// the system <see cref="Type"/> instead.
    /// </summary>
    public static class TypeExtensions
    {
        /// <summary>
        /// Gets a queue name off a system <see cref="Type"/> by using the <see cref="Type"/>.FullName.
        /// If you're in DEBUG with the debugger attached, then the full name is appended by a '-' followed by
        /// an Environment.Username, giving you a named queue during debug cycles to avoid queue name clashes.
        /// </summary>
        /// <param name="type">The message <see cref="Type"/> that this queue is for.</param>
        /// <returns>The final name of the queue.</returns>
        public static string GetQueueName(this Type type)
        {
#if DEBUG
            var queueName = type.FullName;
            if (Debugger.IsAttached)
            {
                queueName += $"-{Environment.UserName}";
            }

            return queueName;
#else
            return type.FullName;
#endif
        }
    }
}

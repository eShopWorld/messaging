using Eshopworld.Messaging;
using Eshopworld.Tests.Core;
using FluentAssertions;
using Xunit;

// ReSharper disable once CheckNamespace
public class ServiceBusExtensionsTest
{
    public class GetNamespaceNameFromConnectionString
    {
        [Fact, IsUnit]
        public void Test_Parse_NormalConnectionString()
        {
            var namespaceName = "my-test-namespace";
            var connectionString = $"Endpoint=sb://{namespaceName}.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SomeFakeKeyNotEncoded=";

            connectionString.GetNamespaceNameFromConnectionString().Should().Be(namespaceName);
        }
    }
}
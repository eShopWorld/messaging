
using Eshopworld.Core;

namespace Eshopworld.Messaging.Tests
{
    public class TestEventWithoutAttribute : DomainEvent { }

    [EswEventName("Test.Event.With.An.Attribute.Defined")]
    public class TestEventWithAttribute : DomainEvent { }

    [EswEventName("")]
    public class TestEventWithEmptyAttributeValue : DomainEvent { }

    [EswEventName("   ")]
    public class TestEvenWithWhiteSpaceAttributeValue : DomainEvent { }
}

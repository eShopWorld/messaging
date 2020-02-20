using Eshopworld.Core;
using Eshopworld.Tests.Core;
using FluentAssertions;
using Xunit;

namespace Eshopworld.Messaging.Tests
{
    public class TypeExtensionsTests
    {
        [Fact, IsLayer0]
        public void GetEntityName_TypeWithoutEswEventNameAttribute_ShouldReturnTypeFullName()
        {
            // Arrange
            var type = typeof(TypeWithoutAttribute);

            // Act
            var entityName = type.GetEntityName();

            // Assert
            entityName.Should().Be(type.FullName?.ToLower());
        }

        [Fact, IsLayer0]
        public void GetEntityName_TypeWithEswEventNameAttribute_ShouldReturnNameProvidedByAttribute()
        {
            // Arrange
            var type = typeof(TypeWithAttribute);

            // Act
            var entityName = type.GetEntityName();

            // Assert
            entityName.Should().Be("test.event.name");
        }

        [Fact, IsLayer0]
        public void GetEntityName_TypeWithEmptyEswEventNameAttributeValue_ShouldReturnTypeFullName()
        {
            // Arrange
            var type = typeof(TypeWithEmptyAttributeValue);

            // Act
            var entityName = type.GetEntityName();

            // Assert
            entityName.Should().Be(type.FullName?.ToLower());
        }

        [Fact, IsLayer0]
        public void GetEntityName_TypeWithWhiteSpaceEswEventNameAttributeValue_ShouldReturnTypeFullName()
        {
            // Arrange
            var type = typeof(TypeWithWhiteSpaceAttributeValue);

            // Act
            var entityName = type.GetEntityName();

            // Assert
            entityName.Should().Be(type.FullName?.ToLower());
        }

        private class TypeWithoutAttribute { }

        [EswEventName("Test.Event.Name")]
        private class TypeWithAttribute { }

        [EswEventName("")]
        private class TypeWithEmptyAttributeValue { }

        [EswEventName("   ")]
        private class TypeWithWhiteSpaceAttributeValue { }
    }
}

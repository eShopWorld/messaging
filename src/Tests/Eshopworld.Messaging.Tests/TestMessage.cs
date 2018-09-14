using System;
using Eshopworld.Tests.Core;

/// <summary>
/// A convenient way to generate random Lorem text around a test message implementation.
/// </summary>
// ReSharper disable once CheckNamespace
public class TestMessage : IEquatable<TestMessage>
{
    private static readonly Random Rng = new Random();

    public string Name { get; set; }

    public string Stuff { get; set; }

    public float Price { get; set; }

    public TestMessage()
    {
        Name = Lorem.GetSentence();
        Stuff = Lorem.GetParagraph();
        Price = Rng.Next(100);
    }

    public TestMessage(string name, string stuff, float price)
    {
        Name = name;
        Stuff = stuff;
        Price = price;
    }

    public bool Equals(TestMessage other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return string.Equals(Name, other.Name) &&
               string.Equals(Stuff, other.Stuff) &&
               Price.Equals(other.Price);
    }
}
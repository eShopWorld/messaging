// ReSharper disable once CheckNamespace
namespace DevOpsFlex.Messaging.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    public static class StringExtensions
    {
        private static readonly Random Rng = new Random();

        public static string Replace(this string str, char item, Func<char> character)
        {
            var builder = new StringBuilder(str.Length);

            foreach (var c in str)
            {
                builder.Append(c == item ? character() : c);
            }

            return builder.ToString();
        }

        public static string Numerify(this string numberString)
        {
            return numberString.Replace('#', () => Rng.Next(10).ToString().ToCharArray()[0]);
        }

        public static string Letterify(this string letterString)
        {
            return letterString.Replace('?', () => 'a'.To('z').Rand());
        }

        public static string Bothify(this string str)
        {
            return Letterify(Numerify(str));
        }

        public static IEnumerable<char> To(this char from, char to)
        {
            for (var i = from; i <= to; i++)
            {
                yield return i;
            }
        }
    }
}

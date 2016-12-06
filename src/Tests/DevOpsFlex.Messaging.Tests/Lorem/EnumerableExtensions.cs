 // ReSharper disable once CheckNamespace
namespace DevOpsFlex.Messaging.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public static class EnumerableExtensions
    {
        private static readonly Random Rng = new Random();

        public static string Join<T>(this IEnumerable<T> items, string separator)
        {
            return items.Select(i => i.ToString())
                        .Aggregate((acc, next) => string.Concat(acc, separator, next));
        }

        public static T Rand<T>(this IEnumerable<T> items)
        {
            var list = items as IList<T> ?? items.ToList();
            return list[Rng.Next(list.Count)];
        }

        public static IEnumerable<T> RandPick<T>(this IEnumerable<T> items, int itemsToTake)
        {
            var list = items as IList<T> ?? items.ToList();

            for (var i = 0; i < itemsToTake; i++)
                yield return list[Rng.Next(list.Count)];
        }

        /// <summary>
        /// From here:
        /// http://stackoverflow.com/questions/375351/most-efficient-way-to-randomly-sort-shuffle-a-list-of-integers-in-c
        /// </summary>
        public static IList<T> Shuffle<T>(this IList<T> array)
        {
            var retArray = new T[array.Count];
            array.CopyTo(retArray, 0);

            for (var i = 0; i < array.Count; i += 1)
            {
                var swapIndex = Rng.Next(i, array.Count);
                if (swapIndex != i)
                {
                    var temp = retArray[i];
                    retArray[i] = retArray[swapIndex];
                    retArray[swapIndex] = temp;
                }
            }

            return retArray;
        }
    }
}
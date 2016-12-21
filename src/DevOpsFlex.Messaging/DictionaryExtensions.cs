namespace DevOpsFlex.Messaging
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// 
    /// </summary>
    public static class DictionaryExtensions
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        public static void Release<TKey, TValue>(this IDictionary<TKey, TValue> source)
            where TValue : IDisposable
        {
            foreach (var key in source.Keys)
            {
                source[key].Dispose();
            }

            source.Clear();
        }
    }
}

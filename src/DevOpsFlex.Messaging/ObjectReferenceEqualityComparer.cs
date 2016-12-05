namespace DevOpsFlex.Messaging
{
    using System.Collections.Generic;
    using System.Runtime.CompilerServices;

    /// <summary>
    /// A generic object comparerer that would only use object's reference, 
    /// ignoring any <see cref="System.IEquatable{T}"/> or <see cref="object.Equals(object)"/>  overrides.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class ObjectReferenceEqualityComparer<T> : EqualityComparer<T>
        where T : class
    {
        private static IEqualityComparer<T> _defaultComparer;

        /// <summary>
        /// Utility static reference to avoid instantiation all the time.
        /// </summary>
        public new static IEqualityComparer<T> Default => _defaultComparer ?? (_defaultComparer = new ObjectReferenceEqualityComparer<T>());

        /// <summary>
        /// Determines whether the specified objects are equal.
        /// </summary>
        /// <param name="x">The first object of type <typeparamref name="T" /> to compare.</param>
        /// <param name="y">The second object of type <typeparamref name="T" /> to compare.</param>
        /// <returns>true if the specified objects are equal; otherwise, false.</returns>
        public override bool Equals(T x, T y)
        {
            return ReferenceEquals(x, y);
        }

        /// <summary>
        /// Returns a hash code for the specified object.
        /// </summary>
        /// <param name="obj">The <see cref="T:System.Object" /> for which a hash code is to be returned.</param>
        /// <returns>A hash code for the specified object.</returns>
        /// <exception cref="T:System.ArgumentNullException">The type of <paramref name="obj" /> is a reference type and <paramref name="obj" /> is null.</exception>
        public override int GetHashCode(T obj)
        {
            return RuntimeHelpers.GetHashCode(obj);
        }
    }
}

namespace DevOpsFlex.Messaging
{
    using System.Collections.Generic;
    using System.Runtime.CompilerServices;

    /// <summary>
    /// A generic object comparerer that would only use object's reference, 
    /// ignoring any <see cref="System.IEquatable{T}"/> or <see cref="object.Equals(object)"/>  overrides.
    /// </summary>
    public class ObjectReferenceEqualityComparer<T> : EqualityComparer<T>
        where T : class
    {
        private static IEqualityComparer<T> _defaultComparer;

        public new static IEqualityComparer<T> Default => _defaultComparer ?? (_defaultComparer = new ObjectReferenceEqualityComparer<T>());

        public override bool Equals(T x, T y)
        {
            return ReferenceEquals(x, y);
        }

        public override int GetHashCode(T obj)
        {
            return RuntimeHelpers.GetHashCode(obj);
        }
    }
}

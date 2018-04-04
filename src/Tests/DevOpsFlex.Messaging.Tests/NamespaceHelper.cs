namespace DevOpsFlex.Messaging.Tests
{
    using System;
    using JetBrains.Annotations;

    public static class NamespaceHelper
    {
        private const string EnvVariable = "devopsflex-sb";
        private static string _sbConnectionString;

        [NotNull] public static string GetConnectionString()
        {
            if (_sbConnectionString != null) return _sbConnectionString;

            _sbConnectionString = Environment.GetEnvironmentVariable(EnvVariable, EnvironmentVariableTarget.User) ??
                                  throw new InvalidOperationException($"Invalid user set environment variable: {EnvVariable}");

            return _sbConnectionString;
        }
    }
}

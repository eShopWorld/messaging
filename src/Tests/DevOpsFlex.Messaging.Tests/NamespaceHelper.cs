using System;

namespace DevOpsFlex.Messaging.Tests
{
    using JetBrains.Annotations;

    public static class NamespaceHelper
    {
        private const string EnvVariable = "devopsflex-sb";
        private static string _sbConnectionString;

        [NotNull]public static string GetConnectionString()
        {
            if (_sbConnectionString != null) return _sbConnectionString;

            var conString = Environment.GetEnvironmentVariable(EnvVariable, EnvironmentVariableTarget.User);
            if (conString == null) throw new InvalidOperationException($"Invalid user set environment variable: {EnvVariable}");

            _sbConnectionString = conString;

            return _sbConnectionString;
        }
    }
}

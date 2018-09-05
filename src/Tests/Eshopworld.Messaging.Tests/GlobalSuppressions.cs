
// This file is used by Code Analysis to maintain SuppressMessage 
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given 
// a specific target and scoped to a namespace, type, member, etc.

[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage(
    category: "Usage",
    checkId: "xUnit1026:Theory methods should use all of their parameters",
    Justification = "They are discards to inject member data as a generics type parameter",
    Scope = "type",
    Target = "MessengerTest")]

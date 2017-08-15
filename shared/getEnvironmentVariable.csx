using System;

public static string getEnvironmentVariable(string name)
{
    var result = System.Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);
    if (result == null)
        return "";
        
    return result; 
}


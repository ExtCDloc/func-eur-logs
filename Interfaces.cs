using System;
using System.Collections.Generic;
using System.Text;

public class JsonFile
{
    public string[] nameFilters;
}

public class MessageItem
{
    public string name;
    public string statecode;
    public string statuscode;
    public DateTime modifiedon;
    public string modifiedby;
    public DateTime createdon;
}

public class FAJsonFile 
{
    public int count;
    public int activeFunctions;
    public FunctionApp[] functionApps;
}

public class FunctionApp 
{
    public string functionAppName;
    public string functionAppStatus;
    public Function[] functions;
}

public class Function 
{
    public string functionName;
    public string statuscode;
    public DateTime modifiedon;
}

public class FunctionResponse 
{
    public string name;
    public bool isDisabled;
}

public class EntitiesRecordResponse
{
    public EntityInfo[] entitiesResponse;
}

public class EntityInfo
{
    public string entityName;
    public DateTime startDate;
    public DateTime endDate;
    public int totalCount;
    public int createdCount;
    public int modifiedCount;
}

public class EntityInfoSettings
{
    public string entityName;
    public string idColumnName;
}
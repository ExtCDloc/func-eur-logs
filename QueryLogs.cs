using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.PowerPlatform.Dataverse.Client;
using System.Collections;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk;
using Ulteam.Common.Logger;
using Azure;

namespace EUR.QueryLog
{
    public static class QueryLogs
    {
        [Function("QueryLogs")]
        public static async Task Run(
            [TimerTrigger("%QueryLogsSchedule%")] TimerInfo myTimer, ILogger logger)
        {
            IDictionary config = Environment.GetEnvironmentVariables();

            using (AzureLogger azureLogger = new AzureLogger(
                instrumentationKey: config["APPINSIGHTS_INSTRUMENTATIONKEY"].ToString(),
                projectName: "func-eur-logs",
                serviceName: "QueryLogs"
            ))
            {
                string sessionId = Guid.NewGuid().ToString();
                azureLogger.CommonProperties.Add("ai_session_id", sessionId);
                azureLogger.TrackEvent("Start QueryLogs");

                string fileName = config["BlobFileName"].ToString();
                var filterValue = await Helper.getValueFromStorage(azureLogger, logger, config, fileName);
                var query = new QueryExpression("plugintracelog")
                {
                    Criteria = new FilterExpression(LogicalOperator.And)
                };
                query.Criteria.AddCondition("createdon", ConditionOperator.Today);

                if (!String.IsNullOrEmpty(filterValue))
                {
                    var dateValue = DateTime.Parse(filterValue);

                    if (dateValue.Date == DateTime.Today.Date)
                    {
                        query.Criteria.AddCondition("createdon", ConditionOperator.GreaterEqual, dateValue);
                    }
                }

                query.ColumnSet = new ColumnSet(true);
                query.AddOrder("createdon", OrderType.Ascending);

                var resultEntities = Helper.RetrieveMultipleEntities(query, azureLogger);

                if (resultEntities != null)
                {
                    azureLogger.TrackEvent($"Total retrieved {resultEntities.Count}");

                    var createdonValue = "";

                    foreach (Entity item in resultEntities)
                    {
                        var properties = new Dictionary<string, string>()
                        {
                            {"EntityName", "plugintracelog"},
                        };
                        
                        foreach (var key in item.Attributes.Keys)
                        {
                            var type = (item.Attributes[key]).GetType();
                            var attrValue = type.ToString() == "Microsoft.Xrm.Sdk.OptionSetValue" ?
                                item.FormattedValues[key].ToString() : item[key].ToString();
                            
                            properties.Add(key, attrValue);
                        }

                        azureLogger.TrackEvent("Log trace record", properties);
                        createdonValue = item["createdon"].ToString();
                    }

                    azureLogger.TrackEvent($"Last date: {createdonValue}");
                    await Helper.setValueToStorage(azureLogger, logger, config, createdonValue, fileName);
                }

                azureLogger.TrackEvent("End QueryLogs");
            }
        }
    }
}


public class MyTableData : Azure.Data.Tables.ITableEntity
{
    public string MyProperty { get; set; }

    public string PartitionKey { get; set; }
    public string RowKey { get; set; }
    public DateTimeOffset? Timestamp { get; set; }
    public ETag ETag { get; set; }
}

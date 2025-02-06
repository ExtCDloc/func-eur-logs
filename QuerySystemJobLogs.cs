using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Collections;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk;
using Ulteam.Common.Logger;

namespace EUR.QueryLog
{
    public static class QuerySystemJobLogs
    {
        [Function("QuerySystemJobLogs")]
        public static async Task Run(
            [TimerTrigger("%QuerySystemJobLogsSchedule%")] TimerInfo myTimer, ILogger logger)
        {
            IDictionary config = Environment.GetEnvironmentVariables();

            using (AzureLogger azureLogger = new AzureLogger(
                instrumentationKey: config["APPINSIGHTS_INSTRUMENTATIONKEY"].ToString(),
                projectName: "func-eur-logs",
                serviceName: "QuerySystemJobLogs"
            ))
            {
                string sessionId = Guid.NewGuid().ToString();
                azureLogger.CommonProperties.Add("ai_session_id", sessionId);
                azureLogger.TrackEvent("Start QuerySystemJobLogs");

                string fileName = config["SystemJobBlobFileName"].ToString();
                var filterValue = await Helper.getValueFromStorage(azureLogger, logger, config, fileName);
                string chunk = config["QuerySystemJobLogs:Chunk"].ToString();

                var query = new QueryExpression("asyncoperation")
                {
                    Criteria = new FilterExpression(LogicalOperator.And),
                    TopCount = Int32.Parse(chunk)
                };

                query.Criteria.AddCondition("createdon", ConditionOperator.LastXDays, "30");
                query.Criteria.AddCondition("operationtype", ConditionOperator.Equal, "1");

                if (!String.IsNullOrEmpty(filterValue))
                {
                    var dateValue = DateTime.Parse(filterValue);
                    query.Criteria.AddCondition("createdon", ConditionOperator.GreaterEqual, dateValue);
                }

                query.ColumnSet = new ColumnSet(true);
                query.AddOrder("createdon", OrderType.Ascending);

                var resultEntities = Helper.RetrieveMultipleEntities(query, azureLogger);

                if (resultEntities != null)
                {
                    azureLogger.TrackEvent($"Total retrieved {resultEntities.Count}");

                    var createdonValue = "";
                    string version = config["QuerySystemJobLogs:Version"].ToString();

                    foreach (Entity item in resultEntities)
                    {
                        var properties = new Dictionary<string, string>()
                        {
                            {"EntityName", "asyncoperation"},
                            {"Version", version},
                        };

                        foreach (var key in item.Attributes.Keys)
                        {
                            var type = (item.Attributes[key]).GetType();
                            var attrValue = type.ToString() switch
                            {
                                "Microsoft.Xrm.Sdk.OptionSetValue" => item.FormattedValues[key].ToString(),
                                "Microsoft.Xrm.Sdk.EntityReference" => ((EntityReference)item.Attributes[key]).Id.ToString(),
                                "Microsoft.Xrm.Sdk.AliasedValue" => ((AliasedValue)item.Attributes[key]).Value.ToString(),
                                _ => item[key].ToString(),
                            };
                            
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
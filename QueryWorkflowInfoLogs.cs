using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.PowerPlatform.Dataverse.Client;
using System.Collections;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk;
using Ulteam.Common.Logger;
using Azure;
using Newtonsoft.Json;

namespace EUR.QueryLog
{
    public static class QueryWorkflowInfoLogs
    {
        [Function("QueryWorkflowInfoLogs")]
        public static async Task Run(
            [TimerTrigger("%QueryWorkflowInfoLogsSchedule%")] TimerInfo myTimer, ILogger logger)
        { 
            IDictionary config = Environment.GetEnvironmentVariables();

            using (AzureLogger azureLogger = new AzureLogger(
                instrumentationKey: config["APPINSIGHTS_INSTRUMENTATIONKEY"].ToString(),
                projectName: "func-eur-logs",
                serviceName: "QueryWorkflowInfoLogs"
            ))
            {
                string sessionId = Guid.NewGuid().ToString();
                azureLogger.CommonProperties.Add("ai_session_id", sessionId);
                azureLogger.TrackEvent("Start QueryWorkflowInfoLogs");

                string fileName = config["WFInfoBlobFileName"].ToString();
                var filterValue = await Helper.getValueFromStorage(azureLogger, logger, config, fileName);
                string[] filter = JsonConvert.DeserializeObject<string[]>(filterValue);
                azureLogger.TrackEvent($"WFs filter value", new Dictionary<string, string>() {{ "filter", filterValue }} );

                if (!String.IsNullOrEmpty(filterValue) && filter.Length > 0)
                {
                    string values = "";
                    foreach (string f in filter)
                    {
                        values += $@"<value>{f}</value>";
                    }

                    string condition = $@"<condition attribute='name' operator='in' >
                                            {values}
                                        </condition>";

                    string fetchquery = $@"<fetch distinct='true'>
                                            <entity name='workflow'>
                                                <attribute name='name' />
                                                <attribute name='category' />
                                                <attribute name='statecode' />
                                                <attribute name='modifiedon' />
                                                <order attribute='name' descending='false' />
                                                <filter type='and'>
                                                    <condition attribute='category' operator='eq' value='0' />
                                                    {condition}
                                                </filter>
                                                <order attribute='modifiedon' descending='true' />
                                            </entity>
                                        </fetch>";

                    var resultEntities = Helper.RetrieveMultipleEntities(new FetchExpression(fetchquery), azureLogger);

                    if (resultEntities != null)
                    {
                        azureLogger.TrackEvent($"Total retrieved {resultEntities.Count}");

                        string version = config["QueryWorkflowInfoLogs:Version"].ToString();

                        foreach (Entity item in resultEntities)
                        {
                            var properties = new Dictionary<string, string>()
                            {
                                {"EntityName", "workflow"},
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

                            azureLogger.TrackEvent("Workflow record", properties);
                        }
                    }
                }

                azureLogger.TrackEvent("End QueryWorkflowInfoLogs");
            }
        }
    }
}
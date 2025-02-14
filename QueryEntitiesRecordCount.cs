using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.PowerPlatform.Dataverse.Client;
using System.Collections;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk;
using Ulteam.Common.Logger;
using Newtonsoft.Json;
using Microsoft.Crm.Sdk.Messages;

namespace EUR.QueryLog
{
    public static class QueryEntitiesRecordCount
    {
        [Function("QueryEntitiesRecordCount")]
        public static async Task Run(
            [TimerTrigger("%QueryEntitiesRecordCountSchedule%")] TimerInfo myTimer, ILogger logger)
        { 
            IDictionary config = Environment.GetEnvironmentVariables();

            using (AzureLogger azureLogger = new AzureLogger(
                instrumentationKey: config["APPINSIGHTS_INSTRUMENTATIONKEY"].ToString(),
                projectName: "func-eur-logs",
                serviceName: "QueryEntitiesRecordCount"
            ))
            {
                string sessionId = Guid.NewGuid().ToString();
                azureLogger.CommonProperties.Add("ai_session_id", sessionId);
                azureLogger.TrackEvent("Start QueryEntitiesRecordCount");

                string fileName = config["EntitiesBlobFileName"].ToString();
                var entityNamesValue = await Helper.getValueFromStorage(azureLogger, logger, config, fileName);
                string[] entityNames = JsonConvert.DeserializeObject<string[]>(entityNamesValue);
                azureLogger.TrackEvent($"Entity names value", new Dictionary<string, string>() {{ "filter", entityNamesValue }} );

                if (!String.IsNullOrEmpty(entityNamesValue) && entityNames.Length > 0)
                {
                    var request = new RetrieveTotalRecordCountRequest { 
                        EntityNames = entityNames
                    };

                    var response = Helper.RetrieveTotalRecordCount(request, azureLogger);

                    if (response != null && response.EntityRecordCountCollection.Count > 0)
                    {
                        foreach (var item in response.EntityRecordCountCollection) {                
                            azureLogger.TrackEvent($"{item.Key} table has {item.Value} records.");            
                        }
                    }
                    else
                    {
                        azureLogger.TrackEvent("EntityRecordCountCollection count: 0");
                    }
                }
            }
        }
    }
}
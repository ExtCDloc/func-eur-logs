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

                try 
                {
                    string entitiesNameFileName = config["EntitiesNameBlobFileName"].ToString();
                    var entitiesNamesValue = await Helper.getValueFromStorage(azureLogger, logger, config, entitiesNameFileName);
                    string[] entityNames = JsonConvert.DeserializeObject<string[]>(entitiesNamesValue);
                    azureLogger.TrackEvent($"Entity names value", new Dictionary<string, string>() {{ "filter entityNames", entitiesNamesValue }} );

                    string entitiesDateFileName = config["EntitiesDateBlobFileName"].ToString();
                    var entitiesDateValue = await Helper.getValueFromStorage(azureLogger, logger, config, entitiesDateFileName);

                    if (!String.IsNullOrEmpty(entitiesNamesValue) && entityNames.Length > 0 && !String.IsNullOrEmpty(entitiesDateValue))
                    {
                        List<EntityInfo> entitiesList = new List<EntityInfo>();
                        DateTime startDate = DateTime.Parse(entitiesDateValue);
                        DateTime endDate = startDate.AddHours(2);

                        foreach (var name in entityNames)
                        {
                            EntityInfo entityInfo = new EntityInfo();
                            entityInfo.entityName = name;
                            entityInfo.totalCount = 0;
                            entityInfo.createdCount = 0;
                            entityInfo.modifiedCount = 0;
                            entityInfo.startDate = startDate;
                            entityInfo.endDate = endDate;
                            entitiesList.Add(entityInfo);
                        }

                        var request = new RetrieveTotalRecordCountRequest { 
                            EntityNames = entityNames
                        };

                        var totalRecordResponse = Helper.RetrieveTotalRecordCount(request, azureLogger);

                        if (totalRecordResponse != null && totalRecordResponse.EntityRecordCountCollection.Count > 0)
                        {
                            foreach (var item in totalRecordResponse.EntityRecordCountCollection) { 
                                entitiesList.FirstOrDefault(i => i.entityName == item.Key).totalCount = (int)item.Value;      
                                azureLogger.TrackEvent($"{item.Key} table has {item.Value} records.");            
                            }
                        }
                        else
                        {
                            azureLogger.TrackEvent("EntityRecordCountCollection count: 0");
                        }

                        List<string> dateAttributes = new List<string> {"createdon", "modifiedon"};
                        foreach (var entityName in entityNames)
                        {
                            foreach (var dateAttribute in dateAttributes)
                            {
                                string fetchquery = $@"<fetch distinct='true'>
                                            <entity name='{entityName}'>
                                                <attribute name='name' />
                                                <attribute name='{dateAttribute}' />
                                                <order attribute='name' descending='false' />
                                                <filter type='and'>
                                                    <condition attribute='{dateAttribute}' operator='ge' value='{startDate}' />
                                                    <condition attribute='{dateAttribute}' operator='le' value='{endDate}' />
                                                </filter>
                                            </entity>
                                        </fetch>";
                                
                                var resultEntities = Helper.RetrieveMultipleEntities(new FetchExpression(fetchquery), azureLogger);
                            
                                if (resultEntities != null)
                                {
                                    if (dateAttribute == "createdon")
                                    {
                                        entitiesList.FirstOrDefault(i => i.entityName == entityName).createdCount = resultEntities.Count;
                                    }
                                    else
                                    {
                                        entitiesList.FirstOrDefault(i => i.entityName == entityName).modifiedCount = resultEntities.Count;
                                    }
                                }
                                else
                                {
                                    azureLogger.TrackEvent($@"RetrieveMultipleEntities count: 0. dateAttribute: {dateAttribute}, entityName: {entityName}");
                                }
                            }
                        }

                        EntitiesRecordResponse response = new EntitiesRecordResponse {
                            entitiesResponse = entitiesList.ToArray()
                        };

                        string version = config["QueryEntitiesRecordCount:Version"].ToString();

                        string jsonResponse = JsonConvert.SerializeObject(response);
                        azureLogger.TrackEvent("Entities info value", new Dictionary<string, string>() 
                        {
                            {"Version", version},
                            {"jsonResponse", jsonResponse},
                        });

                        azureLogger.TrackEvent($"Last date: {endDate}");
                        await Helper.setValueToStorage(azureLogger, logger, config, endDate.ToString(), entitiesDateFileName);
                    }
                }
                catch (Exception ex)
                {
                    azureLogger.TrackException(ex);
                }
            }
        }
    }
}
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
                    var entitySettingsValue = await Helper.getValueFromStorage(azureLogger, logger, config, entitiesNameFileName);
                    EntityInfoSettings[] entitySettings = JsonConvert.DeserializeObject<EntityInfoSettings[]>(entitySettingsValue);
                    azureLogger.TrackEvent($"Entity names value", new Dictionary<string, string>() {{ "filter entityNames", entitySettingsValue }} );

                    string entitiesDateFileName = config["EntitiesDateBlobFileName"].ToString();
                    var entitiesDateValue = await Helper.getValueFromStorage(azureLogger, logger, config, entitiesDateFileName);

                    if (!String.IsNullOrEmpty(entitySettingsValue) 
                        && entitySettings.Length > 0 
                        && !String.IsNullOrEmpty(entitiesDateValue))
                    {
                        DateTime startDate = DateTime.Parse(entitiesDateValue);
                        DateTime endDate = startDate.AddHours(2);

                        if (endDate <= DateTime.Now)
                        {
                            List<EntityInfo> entitiesList = new List<EntityInfo>();

                            foreach (var setting in entitySettings)
                            {
                                EntityInfo entityInfo = new EntityInfo();
                                entityInfo.entityName = setting.entityName;
                                entityInfo.totalCount = 0;
                                entityInfo.createdCount = 0;
                                entityInfo.modifiedCount = 0;
                                entityInfo.startDate = startDate;
                                entityInfo.endDate = endDate;
                                entitiesList.Add(entityInfo);
                            }

                            var request = new RetrieveTotalRecordCountRequest { 
                                EntityNames = Array.ConvertAll(entitySettings, x => x.entityName)
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

                            foreach (var setting in entitySettings)
                            {
                                foreach (var dateAttribute in dateAttributes)
                                {
                                    string fetchquery = $@"<fetch distinct='true'>
                                                <entity name='{setting.entityName}'>
                                                    <attribute name='{setting.idColumnName}' />
                                                    <attribute name='{dateAttribute}' />
                                                    <filter type='and'>
                                                        <condition attribute='{dateAttribute}' operator='ge' value='{startDate}' />
                                                        <condition attribute='{dateAttribute}' operator='le' value='{endDate}' />
                                                    </filter>
                                                </entity>
                                            </fetch>";
                                    azureLogger.TrackEvent($"fetchquery: {fetchquery}");
                                    
                                    var resultEntities = Helper.RetrieveMultipleEntities(new FetchExpression(fetchquery), azureLogger);
                                
                                    if (resultEntities != null)
                                    {
                                        if (dateAttribute == "createdon")
                                        {
                                            entitiesList.FirstOrDefault(i => i.entityName == setting.entityName).createdCount = resultEntities.Count;
                                        }
                                        else
                                        {
                                            entitiesList.FirstOrDefault(i => i.entityName == setting.entityName).modifiedCount = resultEntities.Count;
                                        }
                                    }
                                    else
                                    {
                                        azureLogger.TrackEvent($@"RetrieveMultipleEntities count: 0. dateAttribute: {dateAttribute}, entityName: {setting.entityName}");
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
                        else
                        {
                            azureLogger.TrackEvent($"Current date ({DateTime.Now}) less than end date ({endDate})");
                        }

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
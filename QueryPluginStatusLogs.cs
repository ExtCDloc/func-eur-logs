using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Collections;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk;
using Ulteam.Common.Logger;
using Newtonsoft.Json;

namespace EUR.QueryLog
{
    public static class QueryPluginStatusLogs
    {
        [Function("QueryPluginStatusLogs")]
        public static async Task Run(
            [TimerTrigger("%QueryPluginStatusLogsSchedule%")] TimerInfo myTimer, ILogger logger)
        {
            IDictionary config = Environment.GetEnvironmentVariables();
            
            using (AzureLogger azureLogger = new AzureLogger(
                instrumentationKey: config["APPINSIGHTS_INSTRUMENTATIONKEY"].ToString(),
                projectName: "func-eur-logs",
                serviceName: "QueryPluginStatusLogs"
            ))
            {
                string sessionId = Guid.NewGuid().ToString();
                azureLogger.CommonProperties.Add("ai_session_id", sessionId);
                azureLogger.TrackEvent("Start QueryPluginStatusLogs");

                string fileName = config["PluginStatusBlobFileName"].ToString();
                var filterFile = await Helper.getValueFromStorage(azureLogger, logger, config, fileName);
                string queryFilter = "";

                if (!String.IsNullOrEmpty(filterFile))
                {
                    var filterValues = JsonConvert.DeserializeObject<JsonFile>(filterFile);
                    azureLogger.TrackEvent("Plugin filter value", new Dictionary<string, string>()
                        {
                            { "JsonFile", filterFile }
                        }
                    );

                    if (filterValues.nameFilters.Length > 0) 
                    {
                        string conditions = "";

                        foreach (string name in filterValues.nameFilters) {
                            conditions += $@"<condition attribute='name' operator='eq' value='{name}' />";
                        }

                        queryFilter = $@"<filter type='or'>{conditions}</filter>";
                        azureLogger.TrackEvent("Query filter value", new Dictionary<string, string>()
                            {
                                { "queryFilter", queryFilter }
                            }
                        );
                    }
                    else
                    {
                        azureLogger.TrackEvent($"Name filters length 0");
                    }
                }
                else
                {
                    azureLogger.TrackEvent($"Filter file is empty");
                }
                    
                string fetchquery = $@"<fetch distinct='true'>
                                        <entity name='sdkmessageprocessingstep'>
                                            <attribute name='name' />
                                            <attribute name='statecode' />
                                            <attribute name='statuscode' />
                                            <attribute name='modifiedon' />
                                            <attribute name='createdon' />
                                            <attribute name='modifiedby' />
                                            {queryFilter}
                                            <order attribute='name' />
                                        </entity>
                                    </fetch>";

                try 
                {
                    var resultEntities = Helper.RetrieveMultipleEntities(new FetchExpression(fetchquery), azureLogger);

                    if (resultEntities != null)
                    {
                        azureLogger.TrackEvent($"Total rows retrieved {resultEntities.Count}");
                        string version = config["QueryPluginStatusLogs:Version"].ToString();
                        List<MessageItem> messageItems = new List<MessageItem> { };

                        foreach (Entity item in resultEntities)
                        {    
                            MessageItem messageItem = new MessageItem { };
                            messageItem.name = item["name"].ToString();
                            messageItem.statecode = ((OptionSetValue)item["statecode"]).Value.ToString();
                            messageItem.statuscode = ((OptionSetValue)item["statuscode"]).Value.ToString();
                            messageItem.modifiedon = DateTime.Parse(item["modifiedon"].ToString());
                            messageItem.modifiedby = ((EntityReference)item.Attributes["modifiedby"]).Id.ToString();
                            messageItem.createdon = DateTime.Parse(item["createdon"].ToString());

                            messageItems.Add(messageItem);
                        }

                        var result = messageItems.GroupBy(x => x.name)
                            .Select(group => group.OrderByDescending(x => x.modifiedon).FirstOrDefault())
                            .ToList();

                        azureLogger.TrackEvent($"Result rows retrieved {result.Count}");

                        foreach (var messageItem in result)
                        {
                            var properties = new Dictionary<string, string>()
                            {
                                {"EntityName", "sdkmessageprocessingstep"},
                                {"Version", version},
                                {"name", messageItem.name},
                                {"statecode", messageItem.statecode},
                                {"statuscode", messageItem.statuscode},
                                {"modifiedon", messageItem.modifiedon.ToString()},
                                {"modifiedby", messageItem.modifiedby},
                                {"createdon", messageItem.createdon.ToString()},
                            };

                            azureLogger.TrackEvent("sdkmessageprocessingstep record", properties);
                        }
                    }
                }
                catch (Exception ex)
                {
                    azureLogger.TrackException(ex);
                }

                azureLogger.TrackEvent("End QueryPluginStatusLogs");
            }
        }
    }
}
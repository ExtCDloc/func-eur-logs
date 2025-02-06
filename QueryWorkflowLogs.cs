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
    public static class QueryWorkflowLogs
    {
        [Function("QueryWorkflowLogs")]
        public static async Task Run(
            [TimerTrigger("%QueryWorkflowLogsSchedule%")] TimerInfo myTimer, ILogger logger)
        {    
            IDictionary config = Environment.GetEnvironmentVariables();

            using (AzureLogger azureLogger = new AzureLogger(
                instrumentationKey: config["APPINSIGHTS_INSTRUMENTATIONKEY"].ToString(),
                projectName: "func-eur-logs",
                serviceName: "QueryWorkflowLogs"
            ))
            {
                string sessionId = Guid.NewGuid().ToString();
                azureLogger.CommonProperties.Add("ai_session_id", sessionId);
                azureLogger.TrackEvent("Start QueryWorkflowLogs");

                string fileName = config["WFBlobFileName"].ToString();
                var filterValue = await Helper.getValueFromStorage(azureLogger, logger, config, fileName);
                azureLogger.TrackEvent($"Date filter value: {filterValue}");

                string chunk = config["QueryWorkflowLogs:Chunk"].ToString();
                string filter = !String.IsNullOrEmpty(filterValue) ?  
                    $@"<filter>
                        <condition attribute='createdon' operator='ge' value='{DateTime.Parse(filterValue)}' />
                    </filter>" : "";
                
                string fetchquery = $@"<fetch top='{Int32.Parse(chunk)}' distinct='true'>
                                        <entity name='workflowlog'>
                                            <attribute name='activityname' />
                                            <attribute name='asyncoperationid' />
                                            <attribute name='childworkflowinstanceid' />
                                            <attribute name='completedon' />
                                            <attribute name='createdby' />
                                            <attribute name='createdon' />
                                            <attribute name='createdonbehalfby' />
                                            <attribute name='description' />
                                            <attribute name='duration' />
                                            <attribute name='errorcode' />
                                            <attribute name='errortext' />
                                            <attribute name='iterationcount' />
                                            <attribute name='message' />
                                            <attribute name='modifiedby' />
                                            <attribute name='modifiedon' />
                                            <attribute name='modifiedonbehalfby' />
                                            <attribute name='regardingobjectid' />
                                            <attribute name='repetitioncount' />
                                            <attribute name='repetitionid' />
                                            <attribute name='stagename' />
                                            <attribute name='startedon' />
                                            <attribute name='status' />
                                            <attribute name='stepname' />
                                            <attribute name='workflowlogid' />
                                            {filter}
                                            <order attribute='createdon' />
                                            <link-entity name='asyncoperation' from='asyncoperationid' to='asyncoperationid' link-type='inner' alias='ao'>
                                                <link-entity name='workflow' from='workflowid' to='workflowactivationid' link-type='inner' alias='wf'>
                                                    <attribute name='name' />
                                                    <attribute name='workflowid' />
                                                </link-entity>
                                            </link-entity>
                                        </entity>
                                    </fetch>";


                var resultEntities = Helper.RetrieveMultipleEntities(new FetchExpression(fetchquery), azureLogger);

                if (resultEntities != null)
                {
                    azureLogger.TrackEvent($"Total retrieved {resultEntities.Count}");

                    var createdonValue = "";
                    string version = config["QueryWorkflowLogs:Version"].ToString();

                    foreach (Entity item in resultEntities)
                    {
                        var properties = new Dictionary<string, string>()
                        {
                            {"EntityName", "workflowlog"},
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

                        azureLogger.TrackEvent("Workflow log record", properties);
                        createdonValue = item["createdon"].ToString();
                    }

                    azureLogger.TrackEvent($"Last date: {createdonValue}");
                    await Helper.setValueToStorage(azureLogger, logger, config, createdonValue, fileName);
                }

                azureLogger.TrackEvent("End QueryWorkflowLogs");
            }
        }
    }
}
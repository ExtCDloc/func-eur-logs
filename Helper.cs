using Newtonsoft.Json.Linq;
using System;
using System.Collections;
using System.Text;
using Ulteam.Common.Logger;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage;
using Microsoft.Extensions.Logging;
using System.Net;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Crm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Crm.Sdk.Messages;

namespace EUR.QueryLog
{
    public static class Helper
    {
        public static async Task<string> getValueFromStorage(AzureLogger azureLogger, ILogger _logger, IDictionary config, string fileName)
        {
            string result = string.Empty;

            string connectionString = config["AzureWebJobsStorage"].ToString();
            string containerName = config["BlobContainerName"].ToString();

            try 
            {
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
                CloudBlobClient client = storageAccount.CreateCloudBlobClient();
                CloudBlobContainer container = client.GetContainerReference(containerName);

                CloudBlockBlob blob = container.GetBlockBlobReference($"{fileName}");
                result = blob.DownloadTextAsync().Result;
                azureLogger.TrackEvent($"DownloadTextAsync success. Result: {result}");
            }
            catch (Exception ex)
            {
                azureLogger.TrackException(ex);
            }

            return result;
        }

        public static async Task setValueToStorage(AzureLogger azureLogger, ILogger _logger, IDictionary config, string value, string fileName)
        {
            string connectionString = config["AzureWebJobsStorage"].ToString();
            string containerName = config["BlobContainerName"].ToString();

            try 
            {
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
                CloudBlobClient client = storageAccount.CreateCloudBlobClient();
                CloudBlobContainer container = client.GetContainerReference(containerName);

                CloudBlockBlob blob = container.GetBlockBlobReference($"{fileName}");
                blob.UploadTextAsync(value);
                azureLogger.TrackEvent("UploadTextAsync success");
            }
            catch (Exception ex)
            {
                azureLogger.TrackException(ex);
            }
        }

        public static async Task setFileToStorage(AzureLogger azureLogger, ILogger _logger, IDictionary config, string body, string fileName)
        {
            string connectionString = config["AzureWebJobsStorage"].ToString();
            string containerName = config["BlobContainerName"].ToString();

            try 
            {
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
                CloudBlobClient client = storageAccount.CreateCloudBlobClient();
                CloudBlobContainer container = client.GetContainerReference(containerName);

                CloudBlockBlob blob = container.GetBlockBlobReference($"{fileName}");
                blob.Properties.ContentType = "application/x-www-form-urlencoded";

                using (Stream stream = new MemoryStream(Encoding.UTF8.GetBytes(body)))
                {
                    await blob.UploadFromStreamAsync(stream);
                    azureLogger.TrackEvent("UploadAsync success");
                }
                
            }
            catch (Exception ex)
            {
                azureLogger.TrackException(ex);
            }
        }

        public static HttpClient initHttpClient(string key)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Add("x-functions-key", key);

            return client;
        }

        public static async Task<HttpStatusCode> getFAStatus(AzureLogger azureLogger, string faName)
        {
            HttpClient client = new HttpClient();
            string url = string.Format("https://{0}.azurewebsites.net", faName);
            azureLogger.TrackEvent("getFAStatus url", new Dictionary<string, string>() {{ "url", url }} );
            
            HttpResponseMessage response = await client.GetAsync(url);
            azureLogger.TrackEvent("getFAStatus response", new Dictionary<string, string>() {{ "response", response.ToString() }} );

            return response.StatusCode;
        }

        public static DataCollection<Entity>? RetrieveMultipleEntities(QueryBase query, AzureLogger azureLogger)
        {
            IDictionary config = Environment.GetEnvironmentVariables();
            string connStringConfig = config["EUR:DynamicsConnectionString"].ToString();
            int crmConnConfigUsersCount = GetCrmConnConfigUsersCount(connStringConfig);
            DataCollection<Entity> result = null;

            for (int i = 0; i < crmConnConfigUsersCount; i++)
            {
                try
                {
                    string connString = ConvertCrmConnConfigJsonStringToCrmConnString(connStringConfig, i);
                    using (ServiceClient svc = new ServiceClient(connString))
                    {
                        result = svc.RetrieveMultiple(query).Entities;
                        break;
                    }
                }
                catch
                {
                    azureLogger.TrackEvent($"RetrieveMultipleEntities ServiceClient error: client index {i}");
                }
            }

            return result;
        }

        public static RetrieveTotalRecordCountResponse? RetrieveTotalRecordCount(RetrieveTotalRecordCountRequest request, AzureLogger azureLogger)
        {
            IDictionary config = Environment.GetEnvironmentVariables();
            string connStringConfig = config["EUR:DynamicsConnectionString"].ToString();
            int crmConnConfigUsersCount = GetCrmConnConfigUsersCount(connStringConfig);
            RetrieveTotalRecordCountResponse result = null;

            for (int i = 0; i < crmConnConfigUsersCount; i++)
            {
                try
                {
                    string connString = ConvertCrmConnConfigJsonStringToCrmConnString(connStringConfig, i);
                    IOrganizationService service = new ServiceClient(connString);

                    result = (RetrieveTotalRecordCountResponse)service.Execute(request);
                    break;
                }
                catch
                {
                    azureLogger.TrackEvent($"RetrieveTotalRecordCountResponse ServiceClient error: client index {i}");
                }
            }

            return result;
        }

        public static int GetCrmConnConfigUsersCount(string? crmConnConfigJsonString) {
            int count;

            try
            {
                var crmConnConfig = JObject.Parse(crmConnConfigJsonString);
                var crmUsers = crmConnConfig.Value<JArray>("Users");
                count = crmUsers != null ? crmUsers.Count : 1;
            }
            catch
            {
                count = 0;
            }

            return count;
        }

        public static string ConvertCrmConnConfigJsonStringToCrmConnString(string? crmConnConfigJsonString, int index)
        {
            try
            {
                var crmConnConfig = JObject.Parse(crmConnConfigJsonString);
                var crmUrl = crmConnConfig.Value<string>("CrmUrl")?.TrimEnd('/');
                var crmClientId = crmConnConfig.Value<JArray>("Users")?[index]?.Value<string>("CrmClientId");
                var crmClientSecret = crmConnConfig.Value<JArray>("Users")?[index]?.Value<string>("CrmClientSecret");
                return $"Url={crmUrl};AuthType=ClientSecret;ClientId={crmClientId};ClientSecret={crmClientSecret};RequireNewInstance=false;SkipDiscovery=true;";
            }
            catch
            {
                return crmConnConfigJsonString;
            }
        }
    }
}
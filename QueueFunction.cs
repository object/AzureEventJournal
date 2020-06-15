using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Aliencube.AzureFunctions.Extensions.OpenApi.Attributes;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace AzureEventJournal
{
    public static class QueueFunction
    {
        public const string QueueNameBase = "service-events";
        public const string EventCollectionElementName = "Collection";
        public const string EventPayloadElementName = "EventPayload";
        const int MaxQueueContentLength = 0xFFFF;
        // Restrictions as specified in https://blogs.msdn.microsoft.com/jmstall/2014/06/12/azure-storage-naming-rules/
        public static readonly Regex DisallowedCharsInTableKeys = new Regex(@"[\\\\#%+/?\u0000-\u001F\u007F-\u009F]");

        [FunctionName(nameof(Enqueue))]
        [OpenApiOperation(nameof(Enqueue), "Event")]
        [OpenApiParameter("version",
            In = ParameterLocation.Path, Required = true,
            Type = typeof(string), Description = "API version (e.g. v1)")]
        [OpenApiParameter("collectionName",
            In = ParameterLocation.Path, Required = true,
            Type = typeof(string), Description = "Collection (e.g. omnibus)")]
        [OpenApiRequestBody("application/json", typeof(ProviderEvent))]
        public static async Task<IActionResult> Enqueue(
            [HttpTrigger(AuthorizationLevel.Admin, "post", Route = "queue/{version}/{collectionName}")] HttpRequest req,
            Binder binder,
            string version,
            string collectionName,
            ILogger log)
        {
            var queueName = $"{QueueNameBase}-{version}";
            log.LogInformation($"Posting '{collectionName}' event to '{queueName}' queue.");

            var queue = await binder.BindAsync<CloudQueue>(new Attribute[] { new QueueAttribute($"{queueName}") });
            await queue.CreateIfNotExistsAsync();

            var storage = await Utils.GetStorageAsync(binder, collectionName.ToLower());
            if (storage == null)
                return new NotFoundObjectResult($"Table or blob container '{collectionName}' does not exist");

            var messageBody = await new StreamReader(req.Body).ReadToEndAsync();
            try
            {
                var providerEvent = JsonConvert.DeserializeObject<ProviderEvent>(messageBody);
                if (string.IsNullOrEmpty(providerEvent.Id))
                {
                    return new BadRequestObjectResult($"Event must have either Id or Program");
                }
                if (string.IsNullOrEmpty(providerEvent.Content))
                {
                    return new BadRequestObjectResult($"Event must have non-empty Content");
                }
                if (providerEvent.Created == DateTime.MinValue)
                {
                    return new BadRequestObjectResult($"Event must have valid creation time");
                }
                if (!string.IsNullOrEmpty(providerEvent.Id) && DisallowedCharsInTableKeys.IsMatch(providerEvent.Id))
                {
                    return new BadRequestObjectResult($"Invalid characters in event Id/Program ({providerEvent.Id})");
                }

                var compressedContent = Convert.ToBase64String(Utils.Zip(providerEvent.Content));
                if (compressedContent.Length >= MaxQueueContentLength)
                {
                    await SaveToStorageAsync(storage, collectionName, providerEvent, log);
                }
                else
                {
                    providerEvent.Content = compressedContent;
                    await SendToQueueAsync(queue, collectionName, providerEvent, log);
                }
                return new NoContentResult();
            }
            catch (Exception e)
            {
                return new BadRequestObjectResult(e);
            }
        }

        [FunctionName(nameof(GetQueueMessageCount))]
        [OpenApiOperation(nameof(GetQueueMessageCount), "Count")]
        [OpenApiParameter("version", In = ParameterLocation.Path, Required = true, Type = typeof(string), Description = "API version (e.g. v1)")]
        [OpenApiParameter("queueName", In = ParameterLocation.Path, Required = true, Type = typeof(string), Description = "Name of the Azure queue")]
        [OpenApiResponseBody(HttpStatusCode.OK, "text/plain", typeof(int))]
        public static async Task<IActionResult> GetQueueMessageCount(
            [HttpTrigger(AuthorizationLevel.Admin, "get", Route = "queue/{version}/{queueName}/count")] HttpRequest req,
            Binder binder,
            string version,
            string queueName,
            ILogger log)
        {
            log.LogInformation($"Retrieving '{queueName}' queue count.");

            var queue = await binder.BindAsync<CloudQueue>(new Attribute[] { new QueueAttribute(queueName) });
            if (!await queue.ExistsAsync())
                return new NotFoundObjectResult($"Queue '{queueName}' does not exist");

            await queue.FetchAttributesAsync();

            return new OkObjectResult(queue.ApproximateMessageCount);
        }

        [FunctionName(nameof(QueueTrigger))]
        public static async Task QueueTrigger(
            [QueueTrigger("service-events-v1")] string messageText,
            Binder binder,
            ILogger log)
        {
            try
            {
                var eventEnvelope = JObject.Parse(messageText);
                var providerEvent = new ProviderEvent(ParseEventEnvelope(eventEnvelope, out var collectionName));
                providerEvent.Content = Utils.Unzip(Convert.FromBase64String(providerEvent.Content));

                log.LogInformation($"Saving event to {collectionName}.");

                var storage = await Utils.GetStorageAsync(binder, collectionName.ToLower());
                if (storage == null)
                {
                    log.LogError($"Table or blob container '{collectionName}' does not exist");
                }
                else
                {
                    await SaveToStorageAsync(storage, collectionName, providerEvent, log);
                }
            }
            catch (Exception ex)
            {
                Utils.LogException(ex, log);
                throw;
            }
        }

        private static ProviderEvent ParseEventEnvelope(JObject eventEnvelope, out string collectionName)
        {
            collectionName = eventEnvelope[EventCollectionElementName].Value<string>();
            try
            {
                var doc = eventEnvelope[EventPayloadElementName];
                return doc is JObject ? doc.ToObject<ProviderEvent>() : doc.Value<ProviderEvent>();
            }
            catch (JsonReaderException)
            {
                return JsonConvert.DeserializeObject<ProviderEvent>(eventEnvelope[EventPayloadElementName].Value<string>());
            }
            catch (InvalidCastException)
            {
                return JsonConvert.DeserializeObject<ProviderEvent>(eventEnvelope[EventPayloadElementName].Value<string>());
            }
        }

        private static async Task SendToQueueAsync(CloudQueue queue, string collectionName, ProviderEvent providerEvent, ILogger log)
        {
            var envelope = new Dictionary<string, object>
            {
                {EventCollectionElementName, collectionName},
                {EventPayloadElementName, providerEvent},
            };
            var messageText = JsonConvert.SerializeObject(envelope);
            var message = new CloudQueueMessage(messageText);
            await queue.AddMessageAsync(message);
        }

        private static async Task SaveToStorageAsync(Storage storage, string collectionName, ProviderEvent providerEvent, ILogger log)
        {
            log.LogInformation($"Saving event to {collectionName}.");

            var entityId = providerEvent.Id;

            var entity = new EventEntity(entityId, providerEvent.Created);
            entity.Id = providerEvent.Id;
            entity.Created = providerEvent.Created > DateTime.MinValue ? providerEvent.Created : DateTime.Now;
            entity.Content = Utils.Zip(providerEvent.Content);
            entity.Description = providerEvent.Description;

            await entity.SaveToStorageAsync(storage.Table, storage.BlobContainer);
        }
    }
}

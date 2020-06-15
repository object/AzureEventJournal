using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Aliencube.AzureFunctions.Extensions.OpenApi.Attributes;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Binder = Microsoft.Azure.WebJobs.Binder;

namespace AzureEventJournal
{
    public static class QueryFunction
    {
        [FunctionName(nameof(QueryById))]
        [OpenApiOperation(nameof(QueryById), "Events")]
        [OpenApiParameter("version", In = ParameterLocation.Path, Required = true, Type = typeof(string), Description = "API version (e.g. v1)")]
        [OpenApiParameter("collectionNames", In = ParameterLocation.Path, Required = true, Type = typeof(string), Description = "Comma-separated list of collections")]
        [OpenApiParameter("id", In = ParameterLocation.Path, Required = true, Type = typeof(string), Description = "Id or ProgramId of event documents")]
        [OpenApiParameter("order", In = ParameterLocation.Query, Required = false, Type = typeof(string), Description = "Order (asc/desc)")]
        [OpenApiParameter("top", In = ParameterLocation.Query, Required = false, Type = typeof(int), Description = "Number of results to retrieve")]
        [OpenApiParameter("skip", In = ParameterLocation.Query, Required = false, Type = typeof(int), Description = "Number of results to skip")]
        [OpenApiParameter("noContent", In = ParameterLocation.Query, Required = false, Type = typeof(bool), Description = "Only retrieve event metadata, no payload")]
        [OpenApiParameter("onlyCount", In = ParameterLocation.Query, Required = false, Type = typeof(bool), Description = "Only retrieve event count")]
        [OpenApiResponseBody(HttpStatusCode.OK, "application/json", typeof(JsonResponse))]
        public static async Task<IActionResult> QueryById(
            [HttpTrigger(AuthorizationLevel.Admin, "get", Route = "queryById/{version}/{collectionNames}/{id}")] HttpRequest req,
            Binder binder,
            string version,
            string collectionNames,
            string id,
            ILogger log)
        {
            var querySettings = new QuerySettings(id, new QueryParams(req));
            log.LogInformation($"Retrieving from {collectionNames}/{id}.");

            var storages = new List<Storage>();
            foreach (var collectionName in collectionNames.Split(','))
            {
                var storage = await Utils.GetStorageAsync(binder, collectionName);
                if (storage == null)
                    return new NotFoundObjectResult($"Table or blob container '{collectionName}' does not exist");
                else if (storages.Any(x => x.Table.Name == storage.Table.Name))
                    return new NotFoundObjectResult($"Duplicate collection '{collectionName}'");
                storages.Add(storage);
            }

            var runner = new QueryRunner(storages, querySettings);
            return new OkObjectResult(querySettings.OnlyCount ?
                (object)await runner.GetResultCountAsync() :
                await runner.GetMultipleResultsAsync());
        }

        [FunctionName(nameof(QueryByTime))]
        [OpenApiOperation(nameof(QueryByTime), "Events")]
        [OpenApiParameter("version", In = ParameterLocation.Path, Required = true, Type = typeof(string), Description = "API version (e.g. v1)")]
        [OpenApiParameter("collectionName", In = ParameterLocation.Path, Required = true, Type = typeof(string), Description = "Collection name")]
        [OpenApiParameter("fromTime", In = ParameterLocation.Path, Required = true, Type = typeof(DateTime), Description = "Date/time in ISO8601 format")]
        [OpenApiParameter("toTime", In = ParameterLocation.Path, Required = true, Type = typeof(DateTime), Description = "Date/time in ISO8601 format")]
        [OpenApiParameter("order", In = ParameterLocation.Query, Required = false, Type = typeof(string), Description = "Order (asc/desc)")]
        [OpenApiParameter("top", In = ParameterLocation.Query, Required = false, Type = typeof(int), Description = "Number of results to retrieve")]
        [OpenApiParameter("skip", In = ParameterLocation.Query, Required = false, Type = typeof(int), Description = "Number of results to skip")]
        [OpenApiParameter("noContent", In = ParameterLocation.Query, Required = false, Type = typeof(bool), Description = "Only retrieve event metadata, no payload")]
        [OpenApiParameter("onlyCount", In = ParameterLocation.Query, Required = false, Type = typeof(bool), Description = "Only retrieve event count")]
        [OpenApiResponseBody(HttpStatusCode.OK, "application/json", typeof(JsonResponse))]
        public static async Task<IActionResult> QueryByTime(
            [HttpTrigger(AuthorizationLevel.Admin, "get", Route = "queryByTime/{version}/{collectionName}/{fromTime}/{toTime}")] HttpRequest req,
            Binder binder,
            string version,
            string collectionName,
            string fromTime,
            string toTime,
            ILogger log)
        {
            if (!DateTime.TryParse(fromTime, out var from))
                return new BadRequestObjectResult("Invalid fromTime value");
            if (!DateTime.TryParse(toTime, out var to))
                return new BadRequestObjectResult("Invalid toTime value");
            var querySettings = new QuerySettings(from, to, new QueryParams(req));
            log.LogInformation($"Retrieving from {collectionName}.");

            var storage = await Utils.GetStorageAsync(binder, collectionName);
            if (storage == null)
                return new NotFoundObjectResult($"Table or blob container '{collectionName}' does not exist");

            var runner = new QueryRunner(new[] { storage }, querySettings);
            return new OkObjectResult(querySettings.OnlyCount ?
                (object)await runner.GetResultCountAsync() :
                await runner.GetMultipleResultsAsync());
        }

        [FunctionName(nameof(GetByKey))]
        [OpenApiOperation(nameof(GetByKey), "Event")]
        [OpenApiParameter("version", In = ParameterLocation.Path, Required = true, Type = typeof(string), Description = "API version (e.g. v1)")]
        [OpenApiParameter("collectionName", In = ParameterLocation.Path, Required = true, Type = typeof(string), Description = "Collection name")]
        [OpenApiParameter("partitionKey", In = ParameterLocation.Path, Required = true, Type = typeof(string), Description = "Collection partition key")]
        [OpenApiParameter("rowKey", In = ParameterLocation.Path, Required = true, Type = typeof(string), Description = "Collection row key")]
        [OpenApiResponseBody(HttpStatusCode.OK, "application/json", typeof(JsonResponse))]
        public static async Task<IActionResult> GetByKey(
            [HttpTrigger(AuthorizationLevel.Admin, "get", Route = "getByKey/{version}/{collectionName}/{partitionKey}/{rowKey}")] HttpRequest req,
            Binder binder,
            string version,
            string collectionName,
            string partitionKey,
            string rowKey,
            ILogger log)
        {
            var querySettings = new QuerySettings(partitionKey, rowKey, new QueryParams(req));
            log.LogInformation($"Retrieving from {collectionName}/{partitionKey}/{rowKey}.");

            var storage = await Utils.GetStorageAsync(binder, collectionName);
            if (storage == null)
                return new NotFoundObjectResult($"Table or blob container '{collectionName}' does not exist");

            var runner = new QueryRunner(new[] { storage }, querySettings);
            return new OkObjectResult(await runner.GetSingleResultAsync());
        }
    }

    // Used by OpenApi/Swagger
    class JsonResponse
    {
        public readonly string Collection;
        public string Id { get; set; }
        public string Description { get; set; }
        public DateTime Created { get; set; }
        public string IdPartitionKey { get; set; }
        public string DatePartitionKey { get; set; }
        public string RowKey { get; set; }
        public string Content { get; set; }
    }
}

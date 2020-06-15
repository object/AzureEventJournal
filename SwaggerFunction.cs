using System.Net;
using System.Reflection;
using System.Threading.Tasks;
using Aliencube.AzureFunctions.Extensions.OpenApi;
using Aliencube.AzureFunctions.Extensions.OpenApi.Attributes;
using Aliencube.AzureFunctions.Extensions.OpenApi.Extensions;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi;

namespace AzureEventJournal
{
    public static class SwaggerFunction
    {
        [FunctionName(nameof(RenderOpenApiDocument))]
        [OpenApiIgnore]
        public static async Task<IActionResult> RenderOpenApiDocument(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "openapi/{version}.{extension}")] HttpRequest req,
            string version,
            string extension,
            ILogger log)
        {
            var ver = OpenApiSpecVersion.OpenApi3_0;
            var ext = version == "yaml" ? OpenApiFormat.Yaml : OpenApiFormat.Json;

            var settings = new AppSettings();
            var helper = new DocumentHelper();
            var document = new Document(helper);

            var result = await document.InitialiseDocument()
                .AddMetadata(settings.OpenApiInfo)
                .AddServer(req, settings.HttpSettings.RoutePrefix)
                .Build(Assembly.GetExecutingAssembly())
                .RenderAsync(ver, ext)
                .ConfigureAwait(false);
            var response = new ContentResult()
            {
                Content = result,
                ContentType = "application/json",
                StatusCode = (int)HttpStatusCode.OK
            };

            return response;
        }

        [FunctionName(nameof(RenderSwaggerUI))]
        [OpenApiIgnore]
        public static async Task<IActionResult> RenderSwaggerUI(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "swagger/{version}")] HttpRequest req,
            string version,
            ILogger log)
        {
            var settings = new AppSettings();
            var ui = new SwaggerUI();
            var result = await ui.AddMetadata(settings.OpenApiInfo)
                .AddServer(req, settings.HttpSettings.RoutePrefix)
                .BuildAsync(typeof(SwaggerUI).Assembly)
                .RenderAsync($"openapi/{version}.json", settings.SwaggerAuthKey)
                .ConfigureAwait(false);
            var response = new ContentResult()
            {
                Content = result,
                ContentType = "text/html",
                StatusCode = (int)HttpStatusCode.OK
            };

            return response;
        }
    }
}
using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Functions.Worker.Extensions.Storage.Blobs;
using Azure.Storage.Blobs;
using System.Text;

namespace GlossilyrMovies
{
    public class Function1
    {
        private readonly ILogger _logger;

        public Function1(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<Function1>();
        }

        [Function("Function1")]
        public async Task <HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req,
            [FromBody] string jsonPayload
            )
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            var response = req.CreateResponse(HttpStatusCode.OK);
            response.Headers.Add("Content-Type", "text/plain; charset=utf-8");

            response.WriteString($"Contents of input/input.txt: {jsonPayload}");

            string connString = Environment.GetEnvironmentVariable("connString") ?? string.Empty;

            // Create a blob SERVICE client to connect to the storage account
            BlobServiceClient blobServiceClient = new BlobServiceClient(connString);


            #region blob container client
            // Create a blob CONTAINER client to connect to the container (or create it)
            var blobContainerClient = blobServiceClient.GetBlobContainerClient("films");

            // Create the container if it doesn't exist
            await blobContainerClient.CreateIfNotExistsAsync();

            // Create a BLOB client to create the blob
            var blobClient = blobContainerClient.GetBlobClient($"movies{Guid.NewGuid()}.txt");
            #endregion

            #region blob
            string blobContent = System.Text.Json.JsonSerializer.Serialize(jsonPayload);

            // Convert the string to a byte array (needed for the stream)
            byte[] bytes = Encoding.UTF8.GetBytes(blobContent);

            // Create a stream from the byte array
            using var stream = new MemoryStream(bytes);

            // Upload the stream to the blob
            await blobClient.UploadAsync(stream, overwrite: true);

            return response;
            #endregion
        }
    }
}

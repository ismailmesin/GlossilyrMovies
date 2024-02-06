using System;
using System.IO;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Azure.Storage.Blobs;
using Azure.Messaging.EventGrid;
using Azure;
using System.Reflection.Metadata;
using Newtonsoft.Json;

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
        public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req,
                 [FromBody] string jsonPayload
             )
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            string connString = Environment.GetEnvironmentVariable("connString") ?? "Missing";


            // Create a blob client to connect to the container
            BlobServiceClient blobServiceClient = new BlobServiceClient(connString);
            var blobContainerClient = blobServiceClient.GetBlobContainerClient("films");

            await blobContainerClient.CreateIfNotExistsAsync();

            jsonPayload = jsonPayload.Replace(@"\r\n", "").Replace(Environment.NewLine, "").Replace("\\u0022", "\"");

            //string jsonData = System.Text.Json.JsonSerializer.Serialize(jsonPayload);

            byte[] bytes = Encoding.UTF8.GetBytes(jsonPayload);

            using var stream = new MemoryStream(bytes);

            var blobClient = blobContainerClient.GetBlobClient($"movies{Guid.NewGuid()}.txt");
            await blobClient.UploadAsync(stream, overwrite: true);

            return req.CreateResponse(HttpStatusCode.OK);
        }



        [Function("RunF2")]
        public async Task RunF2([BlobTrigger("films/{id}", Connection = "connString")] Stream stream, string Id)
        {
            _logger.LogInformation("C# Blob trigger function processed a blob.");

            try
            {
                using (StreamReader reader = new StreamReader(stream))
                {
                    string jsonContent = reader.ReadToEnd();

                    // Deserialize JSON to an object with a "movies" property
                    var moviesWrapper = JsonConvert.DeserializeObject<MoviesWrapper>(jsonContent);

                    // Now you have access to the list of movies
                    List<Movie> movies = moviesWrapper?.Movies;

                    if (movies != null)
                    {
                        // Process each movie as needed
                        foreach (var movie in movies)
                        {
                            // Assign a random rating to each movie
                            movie.AssignRandomRating();

                            _logger.LogInformation($"Movie ID: {movie.id}, Name: {movie.name}, Genre: {movie.genre}, Rating: {movie.rating}");
                        }
                    }

                    // Create a new MoviesWrapper object and assign the updated movies list
                    var updatedMoviesWrapper = new MoviesWrapper
                    {
                        Movies = movies
                    };

                    // Serialize the modified movies wrapper back to JSON
                    string updatedJsonData = JsonConvert.SerializeObject(updatedMoviesWrapper);

                    EventGridPublisherClient client = new EventGridPublisherClient(
                new Uri("https://evt-glossilyr.westeurope-1.eventgrid.azure.net/api/events"),
                new AzureKeyCredential("N+y9pFFVUIBVYb0YQRjRvZBR0KKSuMqpV+V6lo1u/d4="));

                    EventGridEvent eventGrid = new EventGridEvent
                        (
                            "UpdatedMovies",
                            "Movies.Modified",
                            "1.0",
                            movies
                        );

                    try
                    {
                        await client.SendEventAsync(eventGrid);
                        _logger.LogInformation("Event published to Event Grid successfully.");
                    }

                    catch (Exception ex)
                    {
                        _logger.LogError("Failed to publish event to Event Grid: {ex.Message}");
                    }

                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error processing blob: {ex.Message}");
            }
        }


    }

    public class MoviesWrapper
    {
        [JsonProperty("movies")]
        public List<Movie> Movies { get; set; }
    }




    public class Movie
    {
        [JsonProperty("id")]
        public string id { get; set; }

        [JsonProperty("name")]
        public string name { get; set; }

        [JsonProperty("genre")]
        public string genre { get; set; }

        [JsonProperty("rating")]
        public int? rating { get; set; } // Make Rating nullable

        public void AssignRandomRating()
        {
            // Generate a random rating between 1 and 5
            Random random = new Random();
            rating = random.Next(1, 6);
        }
    }

}





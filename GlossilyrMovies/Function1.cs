using Azure;
using Azure.Messaging.EventGrid;
using Azure.Messaging.ServiceBus;
using Azure.Storage.Blobs;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Net;
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



        [Function("Function2")]
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
                            moviesWrapper
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

        [Function("Function3")]
        public async Task RunF3([EventGridTrigger] EventGridEvent eventGrid)
        {
            _logger.LogInformation($"Event Grid event received: {eventGrid.Id}");

            try
            {
                // Deserialize the EventGridEvent.Data into MoviesWrapper
                var moviesWrapper = eventGrid.Data.ToObjectFromJson<MoviesWrapper>(); // JsonConvert.DeserializeObject<MoviesWrapper>(eventGrid.Data.ToString());

                if (moviesWrapper != null)
                {
                    // Now you have access to the list of movies
                    List<Movie> movies = moviesWrapper.Movies;

                    if (movies != null)
                    {
                        // Filter movies with a rating of 2 or below
                        var lowRatedMovies = movies.Where(movie => movie.rating.HasValue && movie.rating <= 2).ToList();

                        // Perform actions with the filtered movies
                        foreach (var lowRatedMovie in lowRatedMovies)
                        {
                            _logger.LogInformation($"Low-rated Movie ID: {lowRatedMovie.id}, Name: {lowRatedMovie.name}, Genre: {lowRatedMovie.genre}, Rating: {lowRatedMovie.rating}");
                        }

                        //// the client that owns the connection and can be used to create senders and receivers
                        string serviceBusConnectionString = "Endpoint=sb://sb-glossilyr.servicebus.windows.net/;SharedAccessKeyName=sb-policySend;SharedAccessKey=zcA4F6GD0aDbbOsiUc3FDVM9eAiCBvMoc+ASbLh/LZY=;EntityPath=sbt-glossilyrtopic";

                        // Create a ServiceBusClient
                        ServiceBusClient client = new ServiceBusClient(serviceBusConnectionString);

                        // Create a ServiceBusSender for the topic
                        ServiceBusSender sender = client.CreateSender("sbt-glossilyrtopic");

                        // Create a message using the filtered todo items
                        ServiceBusMessage message = new ServiceBusMessage(JsonConvert.SerializeObject(lowRatedMovies));

                        // Send the message
                        await sender.SendMessageAsync(message);

                        await sender.DisposeAsync();
                        await client.DisposeAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error processing Event Grid event: {ex.Message}");
            }

            _logger.LogInformation("Todo items successfully sent to Service Bus topic.");

        }


        [Function("Function4")]
        public async Task RunF4([ServiceBusTrigger("sbt-glossilyrtopic", "sb-Glossilyr", Connection = "sbConnString")] string message)
        {
            _logger.LogInformation($"Service Bus message received:");

            string cosmosEndpointUrl = "https://db-glossilyr.documents.azure.com:443/";
            string cosmosPrimaryKey = "Lbm12Gvor8hUI9R7sRuXiKhkWzBmgneXgfzPZyep4rC1Alrk5KY5pbqYEPI6ISESFRHLP2tIl8taACDb64YZfA==";

            CosmosClient client = new CosmosClient(cosmosEndpointUrl, cosmosPrimaryKey);
            string databaseName = "db-glossilyr";

            DatabaseResponse dbResponse = await client.CreateDatabaseIfNotExistsAsync(databaseName);
            Console.WriteLine(dbResponse.Database.Id);


            ContainerResponse cResponse = await dbResponse.Database.CreateContainerIfNotExistsAsync("movies2", "/id");
            Console.WriteLine(cResponse.Container.Id);

            var moviesWrapper = JsonConvert.DeserializeObject<List<MoviesWrapper>>(message.ToString());

            foreach (var i in moviesWrapper)
            {

                await cResponse.Container.CreateItemAsync<MoviesWrapper>(i, new PartitionKey(i.id.ToString()));

            }


            _logger.LogInformation("Todo items successfully sent to Cosmos DB.");
        }

    }

    public class MoviesWrapper
    {
        public MoviesWrapper()
        {
            id = Guid.NewGuid();
        }
        public Guid id { get; set; }
        [JsonProperty("movies")]
        public List<Movie> Movies { get; set; }
    }


    public class Movie
    {
        public Movie()
        {
            id = Guid.NewGuid();
        }
        [JsonProperty("id")]
        public Guid id { get; set; }

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





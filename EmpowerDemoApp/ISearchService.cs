using Azure;
using Azure.Search.Documents;
using Azure.Search.Documents.Indexes;
using Azure.Search.Documents.Indexes.Models;
using EmpowerDemoApp.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace EmpowerDemoApp
{
    public interface ISearchService
	{
        Task CreateProductSearchAsync();
        Task DeleteProductSearchAsync();
        Task RunQueryAsync(string query);
    }

    public class SearchService : ISearchService
    {
        private readonly ILogger<SearchService> _logger;
        private readonly IConfiguration _configuration;
        private readonly AzureSettings _azureSettings;

        private string indexerName = "product-sql-idxr";
        private string indexName = "product-sql-idx";

        public SearchService(
            ILogger<SearchService> logger,
            IConfiguration configuration,
            AzureSettings azureSettings)
        {
            _logger = logger;
            _configuration = configuration;
            _azureSettings = azureSettings;
        }

        public async Task CreateProductSearchAsync()
        {
            SearchIndexClient indexClient = new SearchIndexClient(new Uri(_azureSettings.SearchServiceEndPoint), new AzureKeyCredential(_azureSettings.SearchServiceAdminApiKey));
            SearchIndexerClient indexerClient = new SearchIndexerClient(new Uri(_azureSettings.SearchServiceEndPoint), new AzureKeyCredential(_azureSettings.SearchServiceAdminApiKey));

            FieldBuilder fieldBuilder = new FieldBuilder();
            var searchFields = fieldBuilder.Build(typeof(ProductSearch));
            SearchIndex searchIndex = new SearchIndex(indexName, searchFields);
            if (!await AlreadyExistSearchIndex(indexClient))
            {
                Console.WriteLine("create new azure cognitive search index.");
                await indexClient.CreateOrUpdateIndexAsync(searchIndex);
            }

            if (!await AlreadyExistSearchIndexer(indexerClient))
            {
                Console.WriteLine("create new azure cognitive search indexer.");
                await CreateIndexer(indexerClient, searchIndex);
            }
        }

        public async Task DeleteProductSearchAsync()
        {
            Console.WriteLine("remove old azure cognitive search indexer.");
            SearchIndexClient indexClient = new SearchIndexClient(new Uri(_azureSettings.SearchServiceEndPoint), new AzureKeyCredential(_azureSettings.SearchServiceAdminApiKey));
            SearchIndexerClient indexerClient = new SearchIndexerClient(new Uri(_azureSettings.SearchServiceEndPoint), new AzureKeyCredential(_azureSettings.SearchServiceAdminApiKey));
            if (await AlreadyExistSearchIndex(indexClient))
            {
                await indexClient.DeleteIndexAsync(indexName);
            }

            if (await AlreadyExistSearchIndexer(indexerClient))
            {
                await indexerClient.DeleteIndexerAsync(indexerName);
            }
        }

        private async Task<bool> AlreadyExistSearchIndexer(SearchIndexerClient indexerClient)
        {
            try
            {
                return await indexerClient.GetIndexerAsync(indexerName) != null;
            }
            catch (RequestFailedException e) when (e.Status == 404)
            {
                return false;
            }
        }

        private async Task<bool> AlreadyExistSearchIndex(SearchIndexClient indexClient)
        {
            try
            {
                return await indexClient.GetIndexAsync(indexName) != null;
            }
            catch (RequestFailedException e) when (e.Status == 404)
            {
                return false;
            }
        }

        private async Task StartIndexer(SearchIndexerClient indexerClient, SearchIndexer indexer)
        {
            try
            {
                await indexerClient.RunIndexerAsync(indexer.Name);
            }
            catch (RequestFailedException ex) when (ex.Status == 429)
            {

            }
        }

        async Task CheckIndexerStatus(SearchIndexerClient indexerClient, SearchIndexer indexer)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(indexer?.Name))
                {
                    return;
                }
                string indexerName = indexer.Name;
                SearchIndexerStatus execInfo = await indexerClient.GetIndexerStatusAsync(indexerName);
                if (execInfo?.ExecutionHistory != null)
                {
                    Console.WriteLine("Indexer has run {0} times.", execInfo.ExecutionHistory.Count);
                }
                if (execInfo?.Status != null)
                {
                    Console.WriteLine("Indexer Status: " + execInfo.Status.ToString());
                }

                if (execInfo?.LastResult != null)
                {
                    IndexerExecutionResult result = execInfo.LastResult;
                    if (result != null)
                    {
                        Console.WriteLine("Latest run");
                        Console.WriteLine("Run Status: {0}", result.Status.ToString());
                        Console.WriteLine("Total Documents: {0}, Failed: {1}", result.ItemCount, result.FailedItemCount);

                        string errorMsg = (result.ErrorMessage == null) ? "none" : result.ErrorMessage;
                        Console.WriteLine("ErrorMessage: {0}", errorMsg);
                        Console.WriteLine(" Document Errors: {0}, Warnings: {1}\n", result.Errors.Count, result.Warnings.Count);
                    }
                }
            }
            catch (RequestFailedException ex) when (ex.Status == 429)
            {
                _logger.LogError("Failed to run indexer check: {0}", ex.Message);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
            }
        }

        private async Task CreateIndexer(SearchIndexerClient indexerClient, SearchIndex searchIndex)
        {
            var dataSource = new SearchIndexerDataSourceConnection(
             "product-sql-ds",
             SearchIndexerDataSourceType.AzureSql,
             _configuration.GetValue<string>("AzureSQLConnectionString"),
             new SearchIndexerDataContainer(Constent.Products_Table));

            indexerClient.CreateOrUpdateDataSourceConnection(dataSource);

            IndexingSchedule schedule = new IndexingSchedule(TimeSpan.FromDays(1))
            {
                StartTime = DateTimeOffset.Now
            };

            IndexingParameters parameters = new IndexingParameters()
            {
                BatchSize = 100,
                MaxFailedItems = 0,
                MaxFailedItemsPerBatch = 0
            };

            SearchIndexer indexer = new SearchIndexer(indexerName, dataSource.Name, searchIndex.Name)
            {
                Description = "Product Data indexer",
                Schedule = schedule,
                Parameters = parameters,
                FieldMappings =
                {
                    new FieldMapping("product_id") {TargetFieldName = "product_id"},
                    new FieldMapping("product_name") {TargetFieldName = "product_name"},
                    new FieldMapping("category_id") {TargetFieldName = "category_id"},
                    new FieldMapping("price") {TargetFieldName = "price"},
                    new FieldMapping("description") {TargetFieldName = "description"},
                    new FieldMapping("image_url") {TargetFieldName = "image_url"},
                    new FieldMapping("date_added") {TargetFieldName = "date_added"}
                }
            };

            await indexerClient.CreateOrUpdateIndexerAsync(indexer);
            await StartIndexer(indexerClient, indexer);
            await CheckIndexerStatus(indexerClient, indexer);
        }

        public async Task RunQueryAsync(string query)
        {
            // Read the values from appsettings.json
            string searchServiceUri = _azureSettings.SearchServiceEndPoint;
            string queryApiKey = _azureSettings.SearchServiceAdminApiKey;

            // Create a service and index client.
            SearchIndexClient _indexClient = new SearchIndexClient(new Uri(searchServiceUri), new AzureKeyCredential(queryApiKey));
            SearchClient _searchClient = _indexClient.GetSearchClient(indexName);

            var options = new SearchOptions()
            {
                IncludeTotalCount = true
            };

            // Enter Hotel property names to specify which fields are returned.
            // If Select is empty, all "retrievable" fields are returned.
            options.Select.Add("product_id");
            options.Select.Add("product_name");
            options.Select.Add("category_id");
            options.Select.Add("description");
            options.Select.Add("price");
            options.Select.Add("image_url");
            options.Select.Add("date_added");

            // For efficiency, the search call should be asynchronous, so use SearchAsync rather than Search.
            var result = await _searchClient.SearchAsync<ProductSearch>(query, options).ConfigureAwait(false);
            if (result?.Value != null)
            {
                Console.WriteLine($"\nTotal search result count: {result.Value.TotalCount}\n");
                if (result.Value?.GetResults()?.ToList() != null)
                {
                    foreach (var item in result.Value.GetResults().ToList())
                    {
                        if (item?.Document == null)
                        {
                            continue;
                        }
                        Console.WriteLine("\n---------------\n");
                        Console.WriteLine($"{nameof(item.Document.product_id)}: {item.Document.product_id}");
                        Console.WriteLine($"{nameof(item.Document.product_name)}: {item.Document.product_name}");
                        Console.WriteLine($"{nameof(item.Document.category_id)}: {item.Document.category_id}");
                        Console.WriteLine($"{nameof(item.Document.price)}: {item.Document.price}");
                        Console.WriteLine($"{nameof(item.Document.description)}: {item.Document.description}");
                        Console.WriteLine($"{nameof(item.Document.image_url)}: {item.Document.image_url}");
                        Console.WriteLine($"{nameof(item.Document.date_added)}: {item.Document.date_added}");
                    }
                }

                Console.WriteLine($"\nTotal search result count: {result.Value.TotalCount}\n");
            }
            else
            {
                Console.WriteLine($"No result found...");
            }
        }
    }
}


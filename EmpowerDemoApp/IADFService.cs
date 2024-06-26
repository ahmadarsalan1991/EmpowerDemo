﻿using System.Text;
using EmpowerDemoApp.Models;
using Microsoft.Azure.Management.DataFactory;
using Microsoft.Azure.Management.DataFactory.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest;
using Microsoft.Rest.Serialization;

namespace EmpowerDemoApp
{
    public interface IADFService
    {
        Task CreateADFPipline();
    }

    public class ADFService : IADFService
    {
        private readonly IConfiguration _configuration;
        private readonly AzureSettings _azureSettings;
        private readonly BlobStorageSettings _blobStorageSettings;
        private readonly ILogger<ADFService> _logger;
        private readonly IDBService _dBService;
        private readonly ISearchService _searchService;

        private DataFactoryManagementClient client;

        public ADFService(
            IConfiguration configuration,
            AzureSettings azureSettings,
            BlobStorageSettings blobStorageSettings,
            IDBService dBService,
            ISearchService searchService,
            ILogger<ADFService> logger)
        {
            _logger = logger;
            _configuration = configuration;
            _azureSettings = azureSettings;
            _blobStorageSettings = blobStorageSettings;
            _dBService = dBService;
            _searchService = searchService;
        }

        public async Task CreateADFPipline()
        {
            Console.WriteLine("Creating ADF pipline");
            string token = await GetTokenCredentialsAsync();
            client = GetDataFactoryManagementClient(token);
            if (client == null)
            {
                _logger.LogError("Data factory management client is null");
                return;
            }
            await CreateDataFactory();
            await CreateStorageLinkedService();
            await CreateSqlLinkedService();

            await DeleteOldQueuedPipeline(client);

            await CategoryPipeline();
            await ProductPipeline();
            await OrderPipeline();
            await OrderProductPipeline();
            await _searchService.DeleteProductSearchAsync();
            await _searchService.CreateProductSearchAsync();
        }

        private async Task<string> GetTokenCredentialsAsync()
        {
            var context = new AuthenticationContext($"https://login.microsoftonline.com/{_azureSettings.TenantId}");
            var clientCredential = new ClientCredential(_azureSettings.ApplicationClientId, _azureSettings.ApplicationClientSecret);
            var result = await context.AcquireTokenAsync("https://management.azure.com/", clientCredential);
            return result.AccessToken;
        }

        private DataFactoryManagementClient GetDataFactoryManagementClient(string token)
        {
            if (string.IsNullOrWhiteSpace(token))
            {
                return null;
            }
            return new DataFactoryManagementClient(new TokenCredentials(token)) { SubscriptionId = _azureSettings.SubscriptionId };
        }

        private async Task CreateDataFactory()
        {
            Factory factory = new Factory
            {
                Location = _azureSettings.Location,
                Identity = new FactoryIdentity(),
            };
            await client.Factories.CreateOrUpdateAsync(_azureSettings.ResourceGroupName, _azureSettings.DataFactoryName, factory);
            Console.WriteLine(
                SafeJsonConvert.SerializeObject(factory, client.SerializationSettings)
            );
        }

        private async Task CreateStorageLinkedService()
        {
            LinkedServiceResource linkedServiceResource = new LinkedServiceResource(
                new AzureStorageLinkedService
                {
                    ConnectionString = new SecureString($"DefaultEndpointsProtocol=https;AccountName={_blobStorageSettings.StorageAccount};AccountKey={_blobStorageSettings.StorageKey}")
                }
            );

            await client.LinkedServices.CreateOrUpdateAsync(_azureSettings.ResourceGroupName, _azureSettings.DataFactoryName, Constent.StorageLinkedServiceName, linkedServiceResource);
            Console.WriteLine(
                    SafeJsonConvert.SerializeObject(linkedServiceResource, client.SerializationSettings)
                );
        }

        private async Task CreateSqlLinkedService()
        {
            LinkedServiceResource sqlDbLinkedService = new LinkedServiceResource(
                new AzureSqlDatabaseLinkedService
                {
                    ConnectionString = new SecureString(_configuration.GetValue<string>("AzureSQLConnectionString"))
                }
            );
            await client.LinkedServices.CreateOrUpdateAsync(
                _azureSettings.ResourceGroupName, _azureSettings.DataFactoryName, Constent.SqlDbLinkedServiceName, sqlDbLinkedService
            );
            Console.WriteLine(
                SafeJsonConvert.SerializeObject(sqlDbLinkedService, client.SerializationSettings)
            );
        }

        private async Task DeleteOldQueuedPipeline(DataFactoryManagementClient client)
        {
            var runs = await client.PipelineRuns.QueryByFactoryAsync(_azureSettings.ResourceGroupName, _azureSettings.DataFactoryName, new RunFilterParameters());
            if (runs?.Value != null)
            {
                foreach (var run in runs.Value)
                {
                    if (run.Status == "Queued")
                    {
                        Console.WriteLine($"Cancelled run with ID: {run.RunId}");
                        await client.PipelineRuns.CancelAsync(_azureSettings.ResourceGroupName, _azureSettings.DataFactoryName, run.RunId);
                    }
                }
            }
        }

        private async Task CategoryPipeline()
        {
            Console.WriteLine("Creating category blob dataset");
            DatasetResource blobDataset = new DatasetResource(
                new AzureBlobDataset
                {
                    LinkedServiceName = new LinkedServiceReference
                    {
                        ReferenceName = Constent.StorageLinkedServiceName
                    },
                    FolderPath = _blobStorageSettings.ContainerName,
                    FileName = Constent.Categories_Json,
                    Format = new JsonFormat
                    {
                        FilePattern = "arrayOfObjects"
                    },
                    Structure = new List<DatasetDataElement>
                    {
                        new DatasetDataElement { Name = "category_name", Type = "String" }
                    }
                }
            );
            await client.Datasets.CreateOrUpdateAsync(
                _azureSettings.ResourceGroupName, _azureSettings.DataFactoryName, Constent.BlobDatasetName, blobDataset
            );
            Console.WriteLine(
                SafeJsonConvert.SerializeObject(blobDataset, client.SerializationSettings)
            );

            Console.WriteLine("Creating category sql dataset");
            DatasetResource sqlDataset = new DatasetResource(
                new AzureSqlTableDataset
                {
                    LinkedServiceName = new LinkedServiceReference
                    {
                        ReferenceName = Constent.SqlDbLinkedServiceName
                    },
                    TableName = Constent.Categories_Staging_Table
                }
            );

            await client.Datasets.CreateOrUpdateAsync(
                _azureSettings.ResourceGroupName, _azureSettings.DataFactoryName, Constent.SqlDatasetName, sqlDataset
            );
            Console.WriteLine(
                SafeJsonConvert.SerializeObject(sqlDataset, client.SerializationSettings)
            );

            Console.WriteLine("Creating category pipeline");
            PipelineResource pipeline = new PipelineResource
            {
                Activities = new List<Activity>
                {
                    new CopyActivity
                    {
                        Name = "CopyFromBlobToSQL",
                        Inputs = new List<DatasetReference>
                        {
                            new DatasetReference() { ReferenceName = Constent.BlobDatasetName }
                        },
                        Outputs = new List<DatasetReference>
                        {
                            new DatasetReference { ReferenceName = Constent.SqlDatasetName }
                        },
                        Source = new BlobSource { },
                        Sink = new SqlSink { }
                    }
                }
            };

            await client.Pipelines.CreateOrUpdateAsync(_azureSettings.ResourceGroupName, _azureSettings.DataFactoryName, Constent.CategoryPipelineName, pipeline);
            Console.WriteLine(
                SafeJsonConvert.SerializeObject(pipeline, client.SerializationSettings)
            );

            await TriggerPipelineAsync(client, Constent.CategoryPipelineName);
        }

        private async Task ProductPipeline()
        {
            Console.WriteLine("Creating product blob dataset");
            DatasetResource blobDataset = new DatasetResource(
                new AzureBlobDataset
                {
                    LinkedServiceName = new LinkedServiceReference
                    {
                        ReferenceName = Constent.StorageLinkedServiceName
                    },
                    FolderPath = _blobStorageSettings.ContainerName,
                    FileName = Constent.Products_Json,
                    Format = new JsonFormat
                    {
                        FilePattern = "arrayOfObjects"
                    },
                    Structure = new List<DatasetDataElement>
                    {
                        new DatasetDataElement { Name = "product_name", Type = "String" },
                        new DatasetDataElement { Name = "category_id", Type = "Int32" },
                        new DatasetDataElement { Name = "price", Type = "Decimal" },
                        new DatasetDataElement { Name = "description", Type = "String" },
                        new DatasetDataElement { Name = "image_url", Type = "String" },
                        new DatasetDataElement { Name = "date_added", Type = "DateTime" }
                    }
                }
            );
            await client.Datasets.CreateOrUpdateAsync(
                _azureSettings.ResourceGroupName, _azureSettings.DataFactoryName, Constent.BlobDatasetName, blobDataset
            );
            Console.WriteLine(
                SafeJsonConvert.SerializeObject(blobDataset, client.SerializationSettings)
            );

            Console.WriteLine("Creating product sql dataset");
            DatasetResource sqlDataset = new DatasetResource(
                new AzureSqlTableDataset
                {
                    LinkedServiceName = new LinkedServiceReference
                    {
                        ReferenceName = Constent.SqlDbLinkedServiceName
                    },
                    TableName = Constent.Products_Staging_Table
                }
            );

            await client.Datasets.CreateOrUpdateAsync(
                _azureSettings.ResourceGroupName, _azureSettings.DataFactoryName, Constent.SqlDatasetName, sqlDataset
            );
            Console.WriteLine(
                SafeJsonConvert.SerializeObject(sqlDataset, client.SerializationSettings)
            );

            Console.WriteLine("Creating product pipeline");
            PipelineResource pipeline = new PipelineResource
            {
                Activities = new List<Activity>
                {
                    new CopyActivity
                    {
                        Name = "CopyFromBlobToSQL",
                        Inputs = new List<DatasetReference>
                        {
                            new DatasetReference() { ReferenceName = Constent.BlobDatasetName }
                        },
                        Outputs = new List<DatasetReference>
                        {
                            new DatasetReference { ReferenceName = Constent.SqlDatasetName }
                        },
                        Source = new BlobSource { },
                        Sink = new SqlSink { }
                    }
                }
            };

            await client.Pipelines.CreateOrUpdateAsync(_azureSettings.ResourceGroupName, _azureSettings.DataFactoryName, Constent.ProductPipelineName, pipeline);
            Console.WriteLine(
                SafeJsonConvert.SerializeObject(pipeline, client.SerializationSettings)
            );

            await TriggerPipelineAsync(client, Constent.ProductPipelineName);
        }

        private async Task OrderPipeline()
        {
            Console.WriteLine("Creating order blob dataset");
            DatasetResource blobDataset = new DatasetResource(
                new AzureBlobDataset
                {
                    LinkedServiceName = new LinkedServiceReference
                    {
                        ReferenceName = Constent.StorageLinkedServiceName
                    },
                    FolderPath = _blobStorageSettings.ContainerName,
                    FileName = Constent.Orders_Json,
                    Format = new JsonFormat
                    {
                        FilePattern = "arrayOfObjects"
                    },
                    Structure = new List<DatasetDataElement>
                    {
                        new DatasetDataElement { Name = "order_date", Type = "DateTime" },
                        new DatasetDataElement { Name = "customer_name", Type = "String" }
                    }
                }
            );
            await client.Datasets.CreateOrUpdateAsync(
                _azureSettings.ResourceGroupName, _azureSettings.DataFactoryName, Constent.BlobDatasetName, blobDataset
            );
            Console.WriteLine(
                SafeJsonConvert.SerializeObject(blobDataset, client.SerializationSettings)
            );

            Console.WriteLine("Creating order sql dataset");
            DatasetResource sqlDataset = new DatasetResource(
                new AzureSqlTableDataset
                {
                    LinkedServiceName = new LinkedServiceReference
                    {
                        ReferenceName = Constent.SqlDbLinkedServiceName
                    },
                    TableName = Constent.Orders_Staging_Table
                }
            );

            await client.Datasets.CreateOrUpdateAsync(
                _azureSettings.ResourceGroupName, _azureSettings.DataFactoryName, Constent.SqlDatasetName, sqlDataset
            );
            Console.WriteLine(
                SafeJsonConvert.SerializeObject(sqlDataset, client.SerializationSettings)
            );

            Console.WriteLine("Creating order pipeline");
            PipelineResource pipeline = new PipelineResource
            {
                Activities = new List<Activity>
                {
                    new CopyActivity
                    {
                        Name = "CopyFromBlobToSQL",
                        Inputs = new List<DatasetReference>
                        {
                            new DatasetReference() { ReferenceName = Constent.BlobDatasetName }
                        },
                        Outputs = new List<DatasetReference>
                        {
                            new DatasetReference { ReferenceName = Constent.SqlDatasetName }
                        },
                        Source = new BlobSource { },
                        Sink = new SqlSink { }
                    }
                }
            };

            await client.Pipelines.CreateOrUpdateAsync(_azureSettings.ResourceGroupName, _azureSettings.DataFactoryName, Constent.OrderPipelineName, pipeline);
            Console.WriteLine(
                SafeJsonConvert.SerializeObject(pipeline, client.SerializationSettings)
            );

            await TriggerPipelineAsync(client, Constent.OrderPipelineName);
        }

        private async Task OrderProductPipeline()
        {
            Console.WriteLine("Creating product-order blob dataset");
            DatasetResource blobDataset = new DatasetResource(
                new AzureBlobDataset
                {
                    LinkedServiceName = new LinkedServiceReference
                    {
                        ReferenceName = Constent.StorageLinkedServiceName
                    },
                    FolderPath = _blobStorageSettings.ContainerName,
                    FileName = Constent.Order_Products_Json,
                    Format = new JsonFormat
                    {
                        FilePattern = "arrayOfObjects"
                    },
                    Structure = new List<DatasetDataElement>
                    {
                        new DatasetDataElement { Name = "order_id", Type = "Int32" },
                        new DatasetDataElement { Name = "product_id", Type = "Int32" },
                        new DatasetDataElement { Name = "quantity", Type = "Int32" }
                    }
                }
            );
            await client.Datasets.CreateOrUpdateAsync(
                _azureSettings.ResourceGroupName, _azureSettings.DataFactoryName, Constent.BlobDatasetName, blobDataset
            );
            Console.WriteLine(
                SafeJsonConvert.SerializeObject(blobDataset, client.SerializationSettings)
            );

            Console.WriteLine("Creating product-order sql dataset");
            DatasetResource sqlDataset = new DatasetResource(
                new AzureSqlTableDataset
                {
                    LinkedServiceName = new LinkedServiceReference
                    {
                        ReferenceName = Constent.SqlDbLinkedServiceName
                    },
                    TableName = Constent.Order_Products_Staging_Table
                }
            );

            await client.Datasets.CreateOrUpdateAsync(
                _azureSettings.ResourceGroupName, _azureSettings.DataFactoryName, Constent.SqlDatasetName, sqlDataset
            );
            Console.WriteLine(
                SafeJsonConvert.SerializeObject(sqlDataset, client.SerializationSettings)
            );

            Console.WriteLine("Creating product-order pipeline");
            PipelineResource pipeline = new PipelineResource
            {
                Activities = new List<Activity>
                {
                    new CopyActivity
                    {
                        Name = "CopyFromBlobToSQL",
                        Inputs = new List<DatasetReference>
                        {
                            new DatasetReference() { ReferenceName = Constent.BlobDatasetName }
                        },
                        Outputs = new List<DatasetReference>
                        {
                            new DatasetReference { ReferenceName = Constent.SqlDatasetName }
                        },
                        Source = new BlobSource { },
                        Sink = new SqlSink { }
                    }
                }
            };

            await client.Pipelines.CreateOrUpdateAsync(_azureSettings.ResourceGroupName, _azureSettings.DataFactoryName, Constent.OrderProductPipelineName, pipeline);
            Console.WriteLine(
                SafeJsonConvert.SerializeObject(pipeline, client.SerializationSettings)
            );

            await TriggerPipelineAsync(client, Constent.OrderProductPipelineName);
        }

        private async Task TriggerPipelineAsync(DataFactoryManagementClient client, string pipelineName)
        {
            var runResponse = await client.Pipelines.CreateRunWithHttpMessagesAsync(
                    _azureSettings.ResourceGroupName, _azureSettings.DataFactoryName, pipelineName
                );
            Console.WriteLine($"Pipeline run ID: {runResponse.Body.RunId}, PipelineName: {pipelineName}");

            Console.WriteLine($"Checking {pipelineName} pipeline run status...");
            PipelineRun pipelineRun;
            while (true)
            {
                pipelineRun = await client.PipelineRuns.GetAsync(
                    _azureSettings.ResourceGroupName, _azureSettings.DataFactoryName, runResponse.Body.RunId
                );
                Console.WriteLine($"Status {pipelineName} pipeline: {pipelineRun.Status}");
                if (pipelineRun.Status == "InProgress")
                    await Task.Delay(TimeSpan.FromSeconds(3));
                else
                    break;
            }

            RunFilterParameters filterParams = new RunFilterParameters(
                DateTime.UtcNow.AddMinutes(-10), DateTime.UtcNow.AddMinutes(10)
            );

            ActivityRunsQueryResponse queryResponse = client.ActivityRuns.QueryByPipelineRun(
                _azureSettings.ResourceGroupName, _azureSettings.DataFactoryName, runResponse.Body.RunId, filterParams
            );

            try
            {
                if (pipelineRun.Status == "Succeeded")
                {
                    Console.WriteLine(queryResponse.Value.First().Output);
                    if (pipelineName.Equals(Constent.ProductPipelineName))
                    {
                        await _dBService.SyncProductsAsync();
                    }
                    else if (pipelineName.Equals(Constent.CategoryPipelineName))
                    {
                        await _dBService.SyncCategoriesAsync();
                    }
                    else if (pipelineName.Equals(Constent.OrderPipelineName))
                    {
                        await _dBService.SyncOrdersAsync();
                    }
                    else if (pipelineName.Equals(Constent.OrderProductPipelineName))
                    {
                        await _dBService.SyncOrderProductsAsync();
                    }
                }
                else if (pipelineRun.Status == "Queued")
                {
                    await client.PipelineRuns.CancelAsync(_azureSettings.ResourceGroupName, _azureSettings.DataFactoryName, runResponse.Body.RunId);
                    Console.WriteLine($"Cancelled run with ID: {runResponse.Body.RunId}");
                    await TriggerPipelineAsync(client, pipelineName);
                }
                else
                {
                    if (queryResponse?.Value?.First()?.Error != null)
                    {
                        _logger.LogError($"{queryResponse?.Value?.First()?.Error}");
                    }
                    else
                    {
                        Console.WriteLine($"Status {pipelineName} pipeline: {pipelineRun.Status}");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
            }

        }
    }
}


using System;
using System.Text;
using EmpowerDemoApp.Models;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Extensions.Logging;

namespace EmpowerDemoApp
{
	public interface IStorageService
	{
        Task SaveJsonToBlob(string jsonString, string fileName);
    }

    public class StorageService : IStorageService
    {
        private readonly ILogger<StorageService> _logger;
        private readonly BlobStorageSettings _blobStorageSettings;

        public StorageService(
            ILogger<StorageService> logger,
            BlobStorageSettings blobStorageSettings)
        {
            _logger = logger;
            _blobStorageSettings = blobStorageSettings;
        }

        public async Task SaveJsonToBlob(string jsonString, string fileName)
        {
            try
            {
                Console.WriteLine($"Creating blob file {fileName}");
                // Create a blob client
                var storageAccount = CloudStorageAccount.Parse($"DefaultEndpointsProtocol=https;AccountName={_blobStorageSettings.StorageAccount};AccountKey={_blobStorageSettings.StorageKey}");
                var blobClient = storageAccount.CreateCloudBlobClient();
                var container = blobClient.GetContainerReference(_blobStorageSettings.ContainerName);

                // Create the container if it doesn't exist
                await container.CreateIfNotExistsAsync();

                // Get a reference to the blob
                var blockBlob = container.GetBlockBlobReference(fileName);

                // Upload the JSON string as a blob
                using (var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(jsonString)))
                {
                    await blockBlob.UploadFromStreamAsync(memoryStream);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
            }
        }
    }
}


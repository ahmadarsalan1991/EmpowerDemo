using System;
using Newtonsoft.Json;

namespace EmpowerDemoApp.Models
{
    public class BlobStorageSettings
	{
        [JsonProperty("StorageAccount")]
        public string StorageAccount { get; set; }

        [JsonProperty("StorageKey")]
        public string StorageKey { get; set; }

        [JsonProperty("ContainerName")]
        public string ContainerName { get; set; }

        [JsonProperty("CategoryJson")]
        public string CategoryJson { get; set; }

        [JsonProperty("ProductJson")]
        public string ProductJson { get; set; }

        [JsonProperty("OrderJson")]
        public string OrderJson { get; set; }

        [JsonProperty("ProductOrderJson")]
        public string ProductOrderJson { get; set; }
    }
}


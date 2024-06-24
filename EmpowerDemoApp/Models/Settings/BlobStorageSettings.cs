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
    }
}


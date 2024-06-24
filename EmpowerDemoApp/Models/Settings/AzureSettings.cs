using System;
using Newtonsoft.Json;

namespace EmpowerDemoApp.Models
{
    public class AzureSettings
    {
        [JsonProperty("SubscriptionId")]
        public string SubscriptionId { get; set; }

        [JsonProperty("ResourceGroupName")]
        public string ResourceGroupName { get; set; }

        [JsonProperty("DataFactoryName")]
        public string DataFactoryName { get; set; }

        [JsonProperty("SearchServiceEndPoint")]
        public string SearchServiceEndPoint { get; set; }

        [JsonProperty("SearchServiceAdminApiKey")]
        public string SearchServiceAdminApiKey { get; set; }

        [JsonProperty("StorageLinkedServiceName")]
        public string StorageLinkedServiceName { get; set; }

        [JsonProperty("SqlDbLinkedServiceName")]
        public string SqlDbLinkedServiceName { get; set; }

        [JsonProperty("BlobDatasetName")]
        public string BlobDatasetName { get; set; }

        [JsonProperty("SqlDatasetName")]
        public string SqlDatasetName { get; set; }

        [JsonProperty("CategoryPipelineName")]
        public string CategoryPipelineName { get; set; }

        [JsonProperty("ProductPipelineName")]
        public string ProductPipelineName { get; set; }

        [JsonProperty("OrderPipelineName")]
        public string OrderPipelineName { get; set; }

        [JsonProperty("OrderProductPipelineName")]
        public string OrderProductPipelineName { get; set; }

        [JsonProperty("Location")]
        public string Location { get; set; }

        [JsonProperty("TenantId")]
        public string TenantId { get; set; }

        [JsonProperty("ClientId")]
        public string ClientId { get; set; }

        [JsonProperty("ClientSecret")]
        public string ClientSecret { get; set; }
    }
}


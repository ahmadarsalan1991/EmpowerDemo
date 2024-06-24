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

        [JsonProperty("Location")]
        public string Location { get; set; }

        [JsonProperty("TenantId")]
        public string TenantId { get; set; }

        [JsonProperty("ApplicationClientId")]
        public string ApplicationClientId { get; set; }

        [JsonProperty("ApplicationClientSecret")]
        public string ApplicationClientSecret { get; set; }
    }
}


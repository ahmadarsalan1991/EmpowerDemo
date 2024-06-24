using System;
using Newtonsoft.Json;

namespace EmpowerDemoApp.Models
{
	public class SqlTableSettings
	{
        [JsonProperty("CategoryTable")]
        public string CategoryTable { get; set; }

        [JsonProperty("ProductTable")]
        public string ProductTable { get; set; }

        [JsonProperty("OrderTable")]
        public string OrderTable { get; set; }

        [JsonProperty("ProductOrderTable")]
        public string ProductOrderTable { get; set; }
    }
}


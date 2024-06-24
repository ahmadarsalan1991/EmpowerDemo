using Azure.Search.Documents.Indexes;
using Bogus;
using Newtonsoft.Json;

namespace EmpowerDemoApp
{
    public static class Constent
    {
        public const string Categories_Staging_Table = "dbo.Categories_Staging";
        public const string Products_Staging_Table = "dbo.Products_Staging";
        public const string Orders_Staging_Table = "dbo.Orders_Staging";
        public const string Order_Products_Staging_Table = "dbo.Order_Products_Staging";

        public const string Categories_Table = "dbo.Categories";
        public const string Products_Table = "dbo.Products";
        public const string Orders_Table = "dbo.Orders";
        public const string Order_Products_Table = "dbo.Order_Products";

        public const string Categories_Json = "categories.json";
        public const string Products_Json = "products.json";
        public const string Orders_Json = "orders.json";
        public const string Order_Products_Json = "orderproducts.json";

        public const string StorageLinkedServiceName = "StorageLinkedServiceName";
        public const string SqlDbLinkedServiceName = "SqlDbLinkedServiceName";
        public const string BlobDatasetName = "BlobDatasetName";
        public const string SqlDatasetName = "SqlDatasetName";
        public const string CategoryPipelineName = "CategoryPipelineName";
        public const string ProductPipelineName = "ProductPipelineName";
        public const string OrderPipelineName = "OrderPipelineName";
        public const string OrderProductPipelineName = "OrderProductPipelineName";
    }

    public static class DummyDataHelper
    {
        public static List<Category> GenerateRandomCategories(int numberOfCategories)
        {
            var categoryId = 1;
            var categoryFaker = new Faker<Category>()
                .RuleFor(c => c.CategoryId, f => categoryId++)
                .RuleFor(c => c.CategoryName, f => f.Commerce.Categories(1)[0]);

            return categoryFaker.Generate(numberOfCategories);
        }

        public static List<Product> GenerateRandomProducts(int numberOfProducts, List<Category> categories)
        {
            var productId = 1;
            var productFaker = new Faker<Product>()
                .RuleFor(p => p.ProductId, f => productId++)
                .RuleFor(p => p.ProductName, f => f.Commerce.ProductName())
                .RuleFor(p => p.CategoryId, f => f.PickRandom(categories).CategoryId)
                .RuleFor(p => p.Price, f => Convert.ToDecimal(f.Commerce.Price()))
                .RuleFor(p => p.Description, f => f.Lorem.Sentence())
                .RuleFor(p => p.ImageUrl, f => f.Internet.Avatar())
                .RuleFor(p => p.DateAdded, f => f.Date.Past(1).ToString("yyyy-MM-dd"));

            return productFaker.Generate(numberOfProducts);
        }

        public static List<Order> GenerateRandomOrders(int numberOfOrders)
        {
            var orderId = 1;
            var orderFaker = new Faker<Order>()
                .RuleFor(o => o.OrderId, f => orderId++)
                .RuleFor(o => o.OrderDate, f => f.Date.Past(1).ToString("yyyy-MM-dd"))
                .RuleFor(o => o.CustomerName, f => f.Name.FullName());

            return orderFaker.Generate(numberOfOrders);
        }

        public static List<OrderProduct> GenerateRandomOrderProducts(int numberOfOrderProducts, List<Order> orders, List<Product> products)
        {
            HashSet<(int, int)> generatedCombinations = new HashSet<(int, int)>();
            var orderProductFaker = new Faker<OrderProduct>()
            .CustomInstantiator(f =>
            {
                var orderId = f.PickRandom(orders).OrderId;
                var productId = f.PickRandom(products).ProductId;
                // Ensure uniqueness of orderId and productId combination
                while (generatedCombinations.Contains((orderId, productId)))
                {
                    orderId = f.PickRandom(orders).OrderId;
                    productId = f.PickRandom(products).ProductId;
                }
                generatedCombinations.Add((orderId, productId));
                return new OrderProduct
                {
                    OrderId = orderId,
                    ProductId = productId,
                    Quantity = f.Random.Int(1, 5)
                };
            });


            //var orderProductFaker = new Faker<OrderProduct>()
            //    .RuleFor(op => op.OrderId, f => f.PickRandom(orders).OrderId)
            //    .RuleFor(op => op.ProductId, f => f.PickRandom(products).ProductId)
            //    .RuleFor(op => op.Quantity, f => f.Random.Int(1, 5));

            return orderProductFaker.Generate(numberOfOrderProducts);
        }
    }

    public class Category
    {
        [JsonProperty("category_id")]
        public int CategoryId { get; set; }

        [JsonProperty("category_name")]
        public string CategoryName { get; set; }
    }

    public class Product
    {
        [JsonProperty("product_id")]
        public int ProductId { get; set; }

        [SearchableField(IsKey = true, IsFilterable = true, IsSortable = true)]
        [JsonProperty("product_name")]
        public string ProductName { get; set; }

        [JsonProperty("category_id")]
        public int CategoryId { get; set; }

        [SimpleField(IsFilterable = true, IsSortable = true, IsFacetable = true)]
        [JsonProperty("price")]
        public decimal Price { get; set; }

        [SearchableField(IsFilterable = true, IsSortable = true)]
        [JsonProperty("description")]
        public string Description { get; set; }

        [SearchableField(IsFilterable = true, IsSortable = true)]
        [JsonProperty("image_url")]
        public string ImageUrl { get; set; }

        [JsonProperty("date_added")]
        public string DateAdded { get; set; }
    }

    public class Order
    {
        [JsonProperty("order_id")]
        public int OrderId { get; set; }

        [JsonProperty("order_date")]
        public string OrderDate { get; set; }

        [JsonProperty("customer_name")]
        public string CustomerName { get; set; }
    }

    public class OrderProduct
    {
        [JsonProperty("order_id")]
        public int OrderId { get; set; }

        [JsonProperty("product_id")]
        public int ProductId { get; set; }

        [JsonProperty("quantity")]
        public int Quantity { get; set; }
    }

    public class ProductSearch
    {
        [SimpleField(IsKey = true, IsFilterable = true)]
        public string product_id { get; set; } // Changed to string

        [SearchableField(IsSortable = true)]
        public string product_name { get; set; } = null!;

        [SimpleField(IsFilterable = true)]
        public int? category_id { get; set; }

        [SearchableField(IsSortable = true)]
        public string price { get; set; } // Changed to string

        [SearchableField]
        public string description { get; set; }

        [SearchableField]
        public string image_url { get; set; }

        [SimpleField]
        public DateTime date_added { get; set; } // Changed to DateTime
    }
}


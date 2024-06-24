using System.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace EmpowerDemoApp
{
    public interface IDBService
	{
        Task SyncCategoriesAsync();
        Task SyncProductsAsync();
        Task SyncOrdersAsync();
        Task SyncOrderProductsAsync();
        Task<int> GetRecordCountAsync(string tableName);
    }

    public class DBService : IDBService
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<DBService> _logger;
        public DBService(
            ILogger<DBService> logger,
            IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
        }

        public async Task SyncCategoriesAsync()
        {
            string connectionString = _configuration.GetValue<string>("AzureSQLConnectionString");
            string stagingTableName = "Categories_Staging";
            string tableName = "Categories";

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();

                    // Step 1: Insert rows with NULL category_id into the Categories table with new category_id
                    string insertQuery = @$"
                INSERT INTO {tableName} (category_name)
                SELECT category_name
                FROM {stagingTableName}
                WHERE category_id IS NULL;
            ";

                    using (SqlCommand insertCommand = new SqlCommand(insertQuery, connection))
                    {
                        await insertCommand.ExecuteNonQueryAsync();
                    }

                    // Step 2: Merge data from the staging table into the main table, excluding NULL category_id
                    string mergeQuery = @$"
                MERGE INTO {tableName} AS target
                USING (
                    SELECT * FROM {stagingTableName} WHERE category_id IS NOT NULL
                ) AS source
                ON target.category_id = source.category_id
                WHEN MATCHED THEN 
                    UPDATE SET 
                        target.category_name = source.category_name
                WHEN NOT MATCHED BY TARGET THEN 
                    INSERT (category_name)
                    VALUES (source.category_name);
            ";

                    using (SqlCommand mergeCommand = new SqlCommand(mergeQuery, connection))
                    {
                        await mergeCommand.ExecuteNonQueryAsync();
                    }

                    // Clear staging table
                    string truncateQuery = $"TRUNCATE TABLE {stagingTableName};";
                    using (SqlCommand truncateCommand = new SqlCommand(truncateQuery, connection))
                    {
                        await truncateCommand.ExecuteNonQueryAsync();
                    }
                }

                Console.WriteLine($"Sync and reset {stagingTableName} table successfully.");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error syncing and resetting {stagingTableName} table: {ex.Message}");
            }
        }

        public async Task SyncProductsAsync()
        {
            string connectionString = _configuration.GetValue<string>("AzureSQLConnectionString");
            string stagingTableName = "Products_Staging";
            string tableName = "Products";

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();

                    // Step 1: Insert rows with NULL product_id into the Products table with new product_id
                    string insertQuery = @$"
                INSERT INTO {tableName} (product_name, category_id, price, description, image_url, date_added)
                SELECT product_name, category_id, price, description, image_url, date_added
                FROM {stagingTableName}
                WHERE product_id IS NULL;
            ";

                    using (SqlCommand insertCommand = new SqlCommand(insertQuery, connection))
                    {
                        await insertCommand.ExecuteNonQueryAsync();
                    }

                    // Step 2: Merge data from the staging table into the main table, excluding NULL product_id
                    string mergeQuery = @$"
                MERGE INTO {tableName} AS target
                USING (
                    SELECT * FROM {stagingTableName} WHERE product_id IS NOT NULL
                ) AS source
                ON target.product_id = source.product_id
                WHEN MATCHED THEN 
                    UPDATE SET 
                        target.product_name = source.product_name,
                        target.category_id = source.category_id,
                        target.price = source.price,
                        target.description = source.description,
                        target.image_url = source.image_url,
                        target.date_added = source.date_added
                WHEN NOT MATCHED BY TARGET THEN 
                    INSERT (product_name, category_id, price, description, image_url, date_added)
                    VALUES (source.product_name, source.category_id, source.price, source.description, source.image_url, source.date_added);
            ";

                    using (SqlCommand mergeCommand = new SqlCommand(mergeQuery, connection))
                    {
                        await mergeCommand.ExecuteNonQueryAsync();
                    }

                    // Clear staging table
                    string truncateQuery = $"TRUNCATE TABLE {stagingTableName};";
                    using (SqlCommand truncateCommand = new SqlCommand(truncateQuery, connection))
                    {
                        await truncateCommand.ExecuteNonQueryAsync();
                    }
                }

                Console.WriteLine($"Sync and reset {stagingTableName} table successfully.");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error syncing and resetting {stagingTableName} table: {ex.Message}");
            }
        }

        public async Task SyncOrdersAsync()
        {
            string connectionString = _configuration.GetValue<string>("AzureSQLConnectionString");
            string stagingTableName = "Orders_Staging";
            string tableName = "Orders";

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();

                    // Step 1: Insert rows with NULL order_id into the Orders table with new order_id
                    string insertQuery = @$"
                INSERT INTO {tableName} (order_date, customer_name)
                SELECT order_date, customer_name
                FROM {stagingTableName}
                WHERE order_id IS NULL;
            ";

                    using (SqlCommand insertCommand = new SqlCommand(insertQuery, connection))
                    {
                        await insertCommand.ExecuteNonQueryAsync();
                    }

                    // Step 2: Merge data from the staging table into the main table, excluding NULL order_id
                    string mergeQuery = @$"
                MERGE INTO {tableName} AS target
                USING (
                    SELECT * FROM {stagingTableName} WHERE order_id IS NOT NULL
                ) AS source
                ON target.order_id = source.order_id
                WHEN MATCHED THEN 
                    UPDATE SET 
                        target.order_date = source.order_date,
                        target.customer_name = source.customer_name
                WHEN NOT MATCHED BY TARGET THEN 
                    INSERT (order_date, customer_name)
                    VALUES (source.order_date, source.customer_name);
            ";

                    using (SqlCommand mergeCommand = new SqlCommand(mergeQuery, connection))
                    {
                        await mergeCommand.ExecuteNonQueryAsync();
                    }

                    // Clear staging table
                    string truncateQuery = $"TRUNCATE TABLE {stagingTableName};";
                    using (SqlCommand truncateCommand = new SqlCommand(truncateQuery, connection))
                    {
                        await truncateCommand.ExecuteNonQueryAsync();
                    }
                }

                Console.WriteLine($"Sync and reset {stagingTableName} table successfully.");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error syncing and resetting {stagingTableName} table: {ex.Message}");
            }
        }

        public async Task SyncOrderProductsAsync()
        {
            string connectionString = _configuration.GetValue<string>("AzureSQLConnectionString");
            string stagingTableName = "Order_Products_Staging";
            string tableName = "Order_Products";

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();

                    // Step 1: Insert rows with NULL order_id or product_id into the Order_Products table with new order_id or product_id
                    // Note: This step might not be relevant if both order_id and product_id are always provided.
                    string insertQuery = @$"
                INSERT INTO {tableName} (order_id, product_id, quantity)
                SELECT order_id, product_id, quantity
                FROM {stagingTableName}
                WHERE order_id IS NULL OR product_id IS NULL;
            ";

                    using (SqlCommand insertCommand = new SqlCommand(insertQuery, connection))
                    {
                        await insertCommand.ExecuteNonQueryAsync();
                    }

                    // Step 2: Merge data from the staging table into the main table, excluding NULL order_id or product_id
                    string mergeQuery = @$"
                MERGE INTO {tableName} AS target
                USING (
                    SELECT * FROM {stagingTableName} WHERE order_id IS NOT NULL AND product_id IS NOT NULL
                ) AS source
                ON target.order_id = source.order_id AND target.product_id = source.product_id
                WHEN MATCHED THEN 
                    UPDATE SET 
                        target.quantity = source.quantity
                WHEN NOT MATCHED BY TARGET THEN 
                    INSERT (order_id, product_id, quantity)
                    VALUES (source.order_id, source.product_id, source.quantity);
            ";

                    using (SqlCommand mergeCommand = new SqlCommand(mergeQuery, connection))
                    {
                        await mergeCommand.ExecuteNonQueryAsync();
                    }

                    // Clear staging table
                    string truncateQuery = $"TRUNCATE TABLE {stagingTableName};";
                    using (SqlCommand truncateCommand = new SqlCommand(truncateQuery, connection))
                    {
                        await truncateCommand.ExecuteNonQueryAsync();
                    }
                }

                Console.WriteLine($"Sync and reset {stagingTableName} table successfully.");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error syncing and resetting {stagingTableName} table: {ex.Message}");
            }
        }

        public async Task<int> GetRecordCountAsync(string tableName)
        {
            try
            {
                string connectionString = _configuration.GetValue<string>("AzureSQLConnectionString");
                Console.WriteLine($"Checking records for {tableName} table.");

                int recordCount = 0;

                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();

                    using (SqlCommand countCommand = new SqlCommand($"SELECT COUNT(*) FROM {tableName}", connection))
                    {
                        recordCount = (int)await countCommand.ExecuteScalarAsync();
                    }
                }

                Console.WriteLine($"{recordCount} records found in {tableName} table.");
                return recordCount;
            }
            catch (Exception ex)
            {
                _logger.LogError($"{ex.Message}");
                return 0;
            }
        }

    }
}


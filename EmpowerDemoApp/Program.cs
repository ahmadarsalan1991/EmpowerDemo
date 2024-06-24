using EmpowerDemoApp;
using EmpowerDemoApp.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

IConfigurationBuilder builder = new ConfigurationBuilder().AddJsonFile("appsettings.json");
IConfigurationRoot configuration = builder.Build();

IHost _host = Host.CreateDefaultBuilder()
    .ConfigureServices((context, services) =>
    {
        services.AddSingleton<IConfiguration>(configuration);
        services.AddSingleton<IStorageService, StorageService>();
        services.AddSingleton<IADFService, ADFService>();
        services.AddSingleton<IDBService, DBService>();
        services.AddSingleton<ISearchService, SearchService>();

        services.Configure<AzureSettings>(configuration.GetSection("AzureSettings"));
        services.AddSingleton(sp => sp.GetRequiredService<IOptions<AzureSettings>>().Value);

        services.Configure<BlobStorageSettings>(configuration.GetSection("BlobStorageSettings"));
        services.AddSingleton(sp => sp.GetRequiredService<IOptions<BlobStorageSettings>>().Value);

    }).Build();

//DI Resolved
IStorageService storageService = _host.Services.GetRequiredService<IStorageService>();
BlobStorageSettings blobStorageSettings = _host.Services.GetRequiredService<BlobStorageSettings>();
IADFService aDFService = _host.Services.GetRequiredService<IADFService>();
IDBService dBService = _host.Services.GetRequiredService<IDBService>();
ISearchService searchService = _host.Services.GetRequiredService<ISearchService>();


start:
Console.WriteLine("\nEmpowerDemoApp\n");
Console.WriteLine("\n***********************************************\n");
Console.WriteLine("press 1 to run ETL pipeline (ADF)...");
Console.WriteLine("press 2 to use azure cognitive search...");
Console.WriteLine("press 3 to exit app...");
Console.WriteLine("\n***********************************************\n");
string optionInput = Console.ReadLine();
int userOptionInput;
while (!int.TryParse(optionInput, out userOptionInput))
{
    goto start;
}

if (userOptionInput == 1)
{
    Console.WriteLine("Preparing mock data...");

    Console.WriteLine("Number of mock categories you want to create?");
    string categoryNumber = Console.ReadLine();
    int numberOfCategory = 0;
    while (!int.TryParse(categoryNumber, out numberOfCategory))
    {
        Console.WriteLine("Please enter number only");
        categoryNumber = Console.ReadLine();
    }
    List<Category> categories = DummyDataHelper.GenerateRandomCategories(numberOfCategory);

    Console.WriteLine("Number of mock products you want to create?");
    string productNumber = Console.ReadLine();
    int numberOfProduct = 0;
    while (!int.TryParse(productNumber, out numberOfProduct))
    {
        Console.WriteLine("Please enter number only");
        productNumber = Console.ReadLine();
    }
    List<Product> products = DummyDataHelper.GenerateRandomProducts(numberOfProduct, categories);

    Console.WriteLine("Number of mock order you want to create?");
    string orderNumber = Console.ReadLine();
    int numberOfOrder = 0;
    while (!int.TryParse(orderNumber, out numberOfOrder))
    {
        Console.WriteLine("Please enter number only");
        orderNumber = Console.ReadLine();
    }
    List<Order> orders = DummyDataHelper.GenerateRandomOrders(numberOfOrder);

    Console.WriteLine("Number of mock order-product you want to create?");
    string orderProductNumber = Console.ReadLine();
    int numberOfOrderProduct = 0;
    while (!int.TryParse(orderProductNumber, out numberOfOrderProduct))
    {
        Console.WriteLine("Please enter number only");
        orderProductNumber = Console.ReadLine();
    }
    List<OrderProduct> orderProducts = DummyDataHelper.GenerateRandomOrderProducts(numberOfOrderProduct, orders, products);


    await storageService.SaveJsonToBlob(Newtonsoft.Json.JsonConvert.SerializeObject(categories), Constent.Categories_Json);
    await storageService.SaveJsonToBlob(Newtonsoft.Json.JsonConvert.SerializeObject(products), Constent.Products_Json);
    await storageService.SaveJsonToBlob(Newtonsoft.Json.JsonConvert.SerializeObject(orders), Constent.Orders_Json);
    await storageService.SaveJsonToBlob(Newtonsoft.Json.JsonConvert.SerializeObject(orderProducts), Constent.Order_Products_Json);
    await aDFService.CreateADFPipline();

    Console.WriteLine("Press any key to go back to main menu");
    Console.ReadLine();
    goto start;
}
else if (userOptionInput == 2)
{
    int count = await dBService.GetRecordCountAsync("Products");
    if (count < 1)
    {
        Console.WriteLine("No data found...");
        Console.WriteLine("Please run the ETL pipeline before searching...");
        Console.WriteLine("Press any key to go back to main menu");
        Console.ReadLine();
        goto start;
    }
    else
    {
    searchAgaing:
        Console.WriteLine("Plesse enter yor search query");
        string query = Console.ReadLine();
        await searchService.CreateProductSearchAsync();
        await searchService.RunQueryAsync(query);

    searchAgaingInput:
        Console.WriteLine("\n***********************************************");
        Console.WriteLine("Press 1 to search again.");
        Console.WriteLine("Press 2 return to main menu.");
        Console.WriteLine("***********************************************\n");
        string optionSearchInput = Console.ReadLine();
        int userSearchOptionInput;
        while (!int.TryParse(optionSearchInput, out userSearchOptionInput))
        {
            goto searchAgaingInput;
        }

        if (userSearchOptionInput == 1)
        {
            goto searchAgaing;
        }
        else if (userSearchOptionInput == 2)
        {
            goto start;
        }
        else
        {
            goto searchAgaingInput;
        }
    }
}
else if (userOptionInput == 3)
{
    Environment.Exit(0);
}



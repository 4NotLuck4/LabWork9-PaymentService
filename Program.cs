using Confluent.Kafka;
using PaymentService;

bool runSeeder = args.Contains("--seed");
var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
{
    Args = args.Where(arg => arg != "--seed").ToArray(),
    ContentRootPath = AppContext.BaseDirectory
});

string bootstrapServers = builder.Configuration["Kafka:BootstrapServers"]
    ?? throw new InvalidOperationException("Kafka:BootstrapServers is required.");

builder.Services.AddSingleton<IProducer<string, string>>(_ =>
    new ProducerBuilder<string, string>(new ProducerConfig
    {
        BootstrapServers = bootstrapServers,
        EnableIdempotence = true,
        MessageTimeoutMs = 15000
    }).Build());

builder.Services.AddHostedService<PaymentWorker>();
builder.Services.AddSingleton<OrderSeeder>();

using var host = builder.Build();

if (runSeeder)
{
    await host.Services.GetRequiredService<OrderSeeder>().RunAsync();
}
else
{
    await host.RunAsync();
}

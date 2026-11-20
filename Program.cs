using Confluent.Kafka;
using Microsoft.Extensions.Options;
using PaymentService;

bool runSeeder = args.Contains("--seed", StringComparer.Ordinal);
var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
{
    Args = args.Where(arg => arg != "--seed").ToArray(),
    ContentRootPath = AppContext.BaseDirectory
});

builder.Services.AddOptions<KafkaOptions>()
    .BindConfiguration("Kafka")
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.BootstrapServers) &&
        !string.IsNullOrWhiteSpace(options.GroupId) &&
        new[] { options.OrderCreatedTopic, options.PaymentCompletedTopic, options.PaymentFailedTopic }
            .Where(topic => !string.IsNullOrWhiteSpace(topic))
            .Distinct(StringComparer.Ordinal)
            .Count() == 3,
        "Specify Kafka bootstrap servers, a group ID and three distinct topic names.");

builder.Services.AddOptions<PaymentOptions>()
    .BindConfiguration("Payment")
    .Validate(options => options.MaxAmount > 0 && !string.IsNullOrWhiteSpace(options.StateDirectory),
        "Specify a positive maximum amount and a payment state directory.");

builder.Services.AddSingleton<IProducer<string, string>>(services =>
{
    var kafka = services.GetRequiredService<IOptions<KafkaOptions>>().Value;
    return new ProducerBuilder<string, string>(new ProducerConfig
    {
        BootstrapServers = kafka.BootstrapServers,
        EnableIdempotence = true,
        MessageTimeoutMs = 15000
    }).Build();
});

builder.Services.AddHostedService<PaymentWorker>();
builder.Services.AddSingleton<OrderSeeder>();

using var host = builder.Build();

if (runSeeder)
{
    try
    {
        await host.Services.GetRequiredService<OrderSeeder>().RunAsync();
    }
    catch (KafkaException exception)
    {
        host.Services.GetRequiredService<ILogger<OrderSeeder>>()
            .LogError(exception, "Could not publish demo orders.");
        Environment.ExitCode = 1;
    }

    return;
}

await host.RunAsync();

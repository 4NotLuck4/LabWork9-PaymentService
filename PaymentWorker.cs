using System.Text.Json;
using Confluent.Kafka;

namespace PaymentService;

internal sealed class PaymentWorker(
    IProducer<string, string> producer,
    IConfiguration configuration,
    ILogger<PaymentWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = configuration["Kafka:BootstrapServers"],
            GroupId = configuration["Kafka:GroupId"],
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        }).Build();

        try
        {
            consumer.Subscribe(Contracts.OrderCreatedTopic);
            logger.LogInformation("Subscribed to {Topic}", Contracts.OrderCreatedTopic);

            while (!stoppingToken.IsCancellationRequested)
            {
                var message = consumer.Consume(stoppingToken);
                await ProcessAsync(message, stoppingToken);
                consumer.Commit(message);
            }
        }
        finally
        {
            consumer.Close();
        }
    }

    private async Task ProcessAsync(ConsumeResult<string, string> message, CancellationToken stoppingToken)
    {
        OrderCreated order;
        try
        {
            order = JsonSerializer.Deserialize<OrderCreated>(message.Message.Value ?? "null", Contracts.Json)
                ?? throw new JsonException("The order event has no payload.");

            if (string.IsNullOrWhiteSpace(order.OrderId))
            {
                throw new JsonException("The orderId field must be a non-empty string.");
            }
        }
        catch (JsonException exception)
        {
            logger.LogWarning("Skipping invalid order event at {Offset}. {Error}",
                message.TopicPartitionOffset, exception.Message);
            return;
        }

        bool paid = order.Amount > 0;
        var result = new PaymentResult(
            order.OrderId,
            Guid.NewGuid(),
            order.Amount,
            paid ? "completed" : "failed",
            DateTimeOffset.UtcNow,
            paid ? null : "invalid_amount");

        string topic = paid ? Contracts.PaymentCompletedTopic : Contracts.PaymentFailedTopic;
        await producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = order.OrderId,
            Value = JsonSerializer.Serialize(result, Contracts.Json)
        }, stoppingToken);

        logger.LogInformation("Published {Topic} for order {OrderId}", topic, order.OrderId);
    }
}

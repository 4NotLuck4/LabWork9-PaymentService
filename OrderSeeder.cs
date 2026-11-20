using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace PaymentService;

internal sealed class OrderSeeder(
    IProducer<string, string> producer,
    IOptions<KafkaOptions> kafkaOptions,
    IOptions<PaymentOptions> paymentOptions,
    ILogger<OrderSeeder> logger)
{
    public async Task RunAsync()
    {
        string topic = kafkaOptions.Value.OrderCreatedTopic;
        decimal limit = paymentOptions.Value.MaxAmount;
        // Zero always tests the rejection branch, even with a custom upper limit.
        OrderCreated[] orders =
        [
            new($"demo-ok-{Guid.NewGuid():N}", Math.Min(1500m, limit)),
            new($"demo-fail-{Guid.NewGuid():N}", 0m)
        ];

        foreach (var order in orders)
        {
            await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = order.OrderId,
                Value = JsonSerializer.Serialize(order, Contracts.Json)
            });
            logger.LogInformation("Published {Topic} for order {OrderId}, amount {Amount}",
                topic, order.OrderId, order.Amount);
        }
    }
}

using System.Text.Json;
using Confluent.Kafka;

namespace PaymentService;

internal sealed class OrderSeeder(
    IProducer<string, string> producer,
    ILogger<OrderSeeder> logger)
{
    public async Task RunAsync()
    {
        OrderCreated[] orders =
        [
            new($"demo-ok-{Guid.NewGuid():N}", 1500m),
            new($"demo-fail-{Guid.NewGuid():N}", 0m)
        ];

        foreach (var order in orders)
        {
            await producer.ProduceAsync(Contracts.OrderCreatedTopic, new Message<string, string>
            {
                Key = order.OrderId,
                Value = JsonSerializer.Serialize(order, Contracts.Json)
            });
            logger.LogInformation("Published order {OrderId}, amount {Amount}", order.OrderId, order.Amount);
        }
    }
}

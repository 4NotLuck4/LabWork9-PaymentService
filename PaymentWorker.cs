using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace PaymentService;

internal sealed class PaymentWorker(
    IProducer<string, string> producer,
    IOptions<KafkaOptions> kafkaOptions,
    IOptions<PaymentOptions> paymentOptions,
    ILogger<PaymentWorker> logger) : BackgroundService
{
    private readonly KafkaOptions _kafka = kafkaOptions.Value;
    private readonly PaymentOptions _payment = paymentOptions.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();
        using var store = new PaymentStore(_payment.StateDirectory);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ConsumeAsync(store, stoppingToken);
            }
            catch (KafkaException exception) when (!exception.Error.IsFatal)
            {
                logger.LogWarning(exception, "Kafka operation failed. Reconnecting in 3 seconds.");
                await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
            }
        }
    }

    private async Task ConsumeAsync(PaymentStore store, CancellationToken stoppingToken)
    {
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = _kafka.BootstrapServers,
            GroupId = _kafka.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false
        }).Build();

        try
        {
            consumer.Subscribe(_kafka.OrderCreatedTopic);
            logger.LogInformation("Subscribed to {Topic} as {GroupId}",
                _kafka.OrderCreatedTopic, _kafka.GroupId);

            while (!stoppingToken.IsCancellationRequested)
            {
                var message = consumer.Consume(stoppingToken);
                await ProcessAsync(message, store, stoppingToken);
                consumer.Commit(message);
            }
        }
        finally
        {
            consumer.Close();
        }
    }

    private async Task ProcessAsync(
        ConsumeResult<string, string> message,
        PaymentStore store,
        CancellationToken stoppingToken)
    {
        OrderCreated order;
        try
        {
            order = Contracts.ReadOrder(message.Message.Value);
        }
        catch (JsonException exception)
        {
            logger.LogWarning("Skipping invalid order event at {Offset}. {Error}",
                message.TopicPartitionOffset, exception.Message);
            return;
        }

        var saved = store.Find(order.OrderId);
        if (saved is null)
        {
            saved = new SavedPayment(PaymentResult.Simulate(order, _payment.MaxAmount), Published: false);
            store.Save(saved);
        }
        else if (saved.Result.Amount != order.Amount)
        {
            logger.LogWarning(
                "Duplicate order {OrderId} at {Offset} has a different amount. Using the original payment.",
                order.OrderId, message.TopicPartitionOffset);
        }

        if (saved.Published)
        {
            logger.LogInformation("Payment for order {OrderId} was already published", order.OrderId);
            return;
        }

        var result = saved.Result;
        string topic = result.Status == PaymentResult.Completed
            ? _kafka.PaymentCompletedTopic
            : _kafka.PaymentFailedTopic;

        await producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = result.OrderId,
            Value = JsonSerializer.Serialize(result, Contracts.Json)
        }, stoppingToken);

        store.Save(saved with { Published = true });
        logger.LogInformation("Published {Topic} for order {OrderId} and payment {PaymentId}",
            topic, result.OrderId, result.PaymentId);
    }
}

namespace PaymentService;

internal sealed class KafkaOptions
{
    public string BootstrapServers { get; set; } = "";
    public string GroupId { get; set; } = "";
    public string OrderCreatedTopic { get; set; } = "";
    public string PaymentCompletedTopic { get; set; } = "";
    public string PaymentFailedTopic { get; set; } = "";
}

internal sealed class PaymentOptions
{
    public decimal MaxAmount { get; set; }
    public string StateDirectory { get; set; } = "";
}

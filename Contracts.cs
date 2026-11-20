using System.Text.Json;
using System.Text.Json.Serialization;

namespace PaymentService;

internal static class Contracts
{
    public const string OrderCreatedTopic = "order.created";
    public const string PaymentCompletedTopic = "payment.completed";
    public const string PaymentFailedTopic = "payment.failed";

    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        NumberHandling = JsonNumberHandling.Strict,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };
}

internal sealed record OrderCreated(
    [property: JsonRequired] string OrderId,
    [property: JsonRequired] decimal Amount);

internal sealed record PaymentResult(
    string OrderId,
    Guid PaymentId,
    decimal Amount,
    string Status,
    DateTimeOffset ProcessedAtUtc,
    string? Reason);

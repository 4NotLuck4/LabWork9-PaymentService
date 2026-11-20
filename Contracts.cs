using System.Text.Json;
using System.Text.Json.Serialization;

namespace PaymentService;

internal static class Contracts
{
    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        NumberHandling = JsonNumberHandling.Strict,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public static OrderCreated ReadOrder(string? json)
    {
        if (json is null)
        {
            throw new JsonException("The order event has no payload.");
        }

        var order = JsonSerializer.Deserialize<OrderCreated>(json, Json);
        if (order is null || string.IsNullOrWhiteSpace(order.OrderId))
        {
            throw new JsonException("The orderId field must be a non-empty string.");
        }

        return order;
    }
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
    string? Reason)
{
    public const string Completed = "completed";
    public const string Failed = "failed";

    public static PaymentResult Simulate(OrderCreated order, decimal maxAmount)
    {
        string? reason = null;
        if (order.Amount <= 0)
        {
            reason = "invalid_amount";
        }
        else if (order.Amount > maxAmount)
        {
            reason = "amount_limit_exceeded";
        }

        return new PaymentResult(
            order.OrderId,
            Guid.NewGuid(),
            order.Amount,
            reason is null ? Completed : Failed,
            DateTimeOffset.UtcNow,
            reason);
    }
}

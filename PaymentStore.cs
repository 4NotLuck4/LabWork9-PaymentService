using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace PaymentService;

internal sealed record SavedPayment(PaymentResult Result, bool Published);

internal sealed class PaymentStore : IDisposable
{
    private readonly string _directory;
    private readonly FileStream _directoryLock;

    public PaymentStore(string directory)
    {
        _directory = Path.GetFullPath(directory);
        Directory.CreateDirectory(_directory);
        _directoryLock = new FileStream(Path.Combine(_directory, "worker.lock"),
            FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
    }

    public SavedPayment? Find(string orderId)
    {
        string path = GetPath(orderId);
        try
        {
            using var file = File.OpenRead(path);
            return JsonSerializer.Deserialize<SavedPayment>(file, Contracts.Json)
                ?? throw new InvalidDataException($"Payment state is empty: {path}");
        }
        catch (FileNotFoundException)
        {
            return null;
        }
    }

    public void Save(SavedPayment payment)
    {
        string path = GetPath(payment.Result.OrderId);
        string temporaryPath = path + ".tmp";

        using (var file = File.Create(temporaryPath))
        {
            JsonSerializer.Serialize(file, payment, Contracts.Json);
            file.Flush(flushToDisk: true);
        }

        File.Move(temporaryPath, path, overwrite: true);
    }

    private string GetPath(string orderId)
    {
        byte[] hash = SHA256.HashData(Encoding.UTF8.GetBytes(orderId));
        return Path.Combine(_directory, Convert.ToHexString(hash) + ".json");
    }

    public void Dispose() => _directoryLock.Dispose();
}

# Лабораторная работа № 9. Сервис оплаты

Сервис № 3 написан на C# (.NET 8 Worker Service). Он читает заказы из `order.created`. Положительная сумма даёт `payment.completed`, нулевая или отрицательная — `payment.failed`.

## Структура

| Файл | Назначение |
| --- | --- |
| `Program.cs` | Настраивает Kafka Producer и запускает Worker или сидер |
| `PaymentWorker.cs` | Читает заказы, имитирует оплату и публикует результат |
| `Contracts.cs` | Описывает топики и JSON-модели событий |
| `OrderSeeder.cs` | Отправляет два тестовых заказа |
| `appsettings.json` | Задаёт адрес брокера и группу потребителей |
| `PaymentService.csproj` | Задаёт .NET 8 и зависимости проекта |

## Запуск и проверка

Установите .NET 8 SDK. Брокер из задания — `10.9.2.80:9092`. Если автоматическое создание топиков отключено, заранее создайте `order.created`, `payment.completed` и `payment.failed`.

Из каталога проекта запустите Worker:

```bash
dotnet run
```

Во втором терминале из того же каталога запустите сидер:

```bash
dotnet run -- --seed
```

Сидер публикует заказ на 1500 и заказ на 0 с уникальными идентификаторами. Сопоставьте эти идентификаторы с записями Worker о публикации в `payment.completed` и `payment.failed`.

Для локального брокера добавьте к командам параметр `--Kafka:BootstrapServers=localhost:9092`. Для Worker команда будет `dotnet run -- --Kafka:BootstrapServers=localhost:9092`, для сидера — `dotnet run -- --seed --Kafka:BootstrapServers=localhost:9092`.

## JSON для интеграции

Входное событие `order.created` содержит строковый `orderId` и числовой `amount`.

```json
{"orderId":"order-101","amount":1500}
```

Пример `payment.completed`:

```json
{"orderId":"order-101","paymentId":"203a0b81-f7f1-4a63-8f6a-5147eed03432","amount":1500,"status":"completed","processedAtUtc":"2026-11-20T08:30:00+00:00"}
```

В `payment.failed` значение `status` равно `failed`, дополнительно передаётся `"reason":"invalid_amount"`. Ключ выходного события — `orderId`. Невалидное входное сообщение записывается в журнал и пропускается. При повторной доставке заказа сервис может отправить новый результат для того же `orderId`.

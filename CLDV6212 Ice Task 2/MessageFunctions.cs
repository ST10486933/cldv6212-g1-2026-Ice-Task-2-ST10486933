using System.Text.Json;
using Azure.Data.Tables;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace MessagePipeline;

public record MessageEnvelope(string Id, DateTimeOffset ReceivedAt, string Payload);

public class MessageFunctions
{
    public const string QueueName = "incoming-messages";
    public const string TableName = "Messages";
    public const string Conn = "AzureWebJobsStorage";

    private readonly ILogger<MessageFunctions> _logger;

    public MessageFunctions(ILogger<MessageFunctions> logger) => _logger = logger;

    // Stage 1: HTTP (Postman) -> Queue
    [Function("HttpToQueue")]
    [QueueOutput(QueueName, Connection = Conn)]
    public async Task<string> HttpToQueue(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "ingest")] HttpRequest req)
    {
        using var reader = new StreamReader(req.Body);
        var body = await reader.ReadToEndAsync();

        var envelope = new MessageEnvelope(Guid.NewGuid().ToString(), DateTimeOffset.UtcNow, body);
        _logger.LogInformation("Queued message {Id}", envelope.Id);

        return JsonSerializer.Serialize(envelope);
    }

    // Stage 2: Queue -> Table Storage
    [Function("QueueToTable")]
    [TableOutput(TableName, Connection = Conn)]
    public TableEntity QueueToTable(
        [QueueTrigger(QueueName, Connection = Conn)] string message)
    {
        var envelope = JsonSerializer.Deserialize<MessageEnvelope>(message)
            ?? throw new InvalidOperationException("Queue message was empty");

        var entity = new TableEntity(envelope.ReceivedAt.ToString("yyyy-MM-dd"), envelope.Id)
        {
            ["ReceivedAt"] = envelope.ReceivedAt,
            ["Payload"] = envelope.Payload
        };

        _logger.LogInformation("Writing message {Id} to table {Table}", envelope.Id, TableName);
        return entity;
    }
}
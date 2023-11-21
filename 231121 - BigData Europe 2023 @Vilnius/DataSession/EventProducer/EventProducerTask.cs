using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using EventProducer;
using System.Text;
using System.Text.Json;

internal class EventProducerTask
{
    private string _ehConnection = "omitted";
    
    internal async Task RunAsync()
    {
        var service = new SourcingService();
        var startDate = new DateTime(2023, 10, 1);
        var conversions = service.ConversionEvents(startDate).SelectMany(p=>p).ToArray();
        var clicks = service.ClicksEvents(startDate).SelectMany(p => p).ToArray();
        var impressions= service.ImpressionEvents(startDate).SelectMany(p => p).ToArray();

        var justForDebug=JsonSerializer.Serialize(conversions,new JsonSerializerOptions { WriteIndented=true});
        await SendViaEventHub(conversions);
        await SendViaEventHub(clicks);
        await SendViaEventHub(impressions);
    }

    private async Task SendViaEventHub(IEnumerable<object> data)
    {
        var producerClient = new EventHubProducerClient(_ehConnection);
        EventDataBatch currentBatch = null;
        foreach (var item in data)
        {
            if (currentBatch == null) currentBatch = await producerClient.CreateBatchAsync();
            if (!currentBatch.TryAdd(
                new EventData(
                    (Encoding.UTF8.GetBytes
                        (JsonSerializer.Serialize(item))))))
            {
                await Console.Out.WriteLineAsync($"Sending batch of {currentBatch.Count} events");
                await producerClient.SendAsync(currentBatch);
                currentBatch = null;
            }
        }
        await Console.Out.WriteLineAsync($"Sending batch of {currentBatch?.Count} events");
        if (currentBatch!=null) await producerClient.SendAsync(currentBatch);

    }
}
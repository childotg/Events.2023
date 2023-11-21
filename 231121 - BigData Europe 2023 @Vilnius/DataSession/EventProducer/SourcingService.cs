using EventProducer.Models;

namespace EventProducer
{
    public class SourcingService
    {
        public IEnumerable<(DateTime, ConversionEvent[], ClickEvent[], ImpressionEvent[])> Reports(DateTime startDate)
        {
            var endDate = DateTime.Today;
            var currentDate = startDate;
            while (currentDate < endDate)
            {
                Console.WriteLine($"Generating {currentDate}");
                var conversions = Enumerable.Range(0, Utils.Random(100, 10000))
                    .Select(p => ConversionEvent.Create(currentDate)).ToArray();
                var clicks = Enumerable.Range(0, Utils.Random(100, 10000))
                    .Select(p => ClickEvent.Create(currentDate)).ToArray();
                var impressions = Enumerable.Range(0, Utils.Random(100, 1000))
                    .Select(p => ImpressionEvent.Create(currentDate)).ToArray();
                
                yield return (currentDate, conversions, clicks, impressions);
                currentDate = currentDate.AddDays(1);
            }
        }

        public IEnumerable<ConversionEvent[]> ConversionEvents(DateTime startDate)
        {
            var endDate = DateTime.Today;
            var currentDate = startDate;
            while (currentDate < endDate)
            {
                Console.WriteLine($"Generating {currentDate}");
                var conversions = Enumerable.Range(0, Utils.Random(100, 10000))
                    .Select(p => ConversionEvent.Create(currentDate)).ToArray();
                
                yield return conversions;
                currentDate = currentDate.AddDays(1);
            }
        }

        public IEnumerable<ClickEvent[]> ClicksEvents(DateTime startDate)
        {
            var endDate = DateTime.Today;
            var currentDate = startDate;
            while (currentDate < endDate)
            {
                Console.WriteLine($"Generating {currentDate}");
                var clicks = Enumerable.Range(0, Utils.Random(100, 10000))
                    .Select(p => ClickEvent.Create(currentDate)).ToArray();

                yield return clicks;
                currentDate = currentDate.AddDays(1);
            }
        }

        public IEnumerable<ImpressionEventFlatten[]> ImpressionEvents(DateTime startDate)
        {
            var endDate = DateTime.Today;
            var currentDate = startDate;
            while (currentDate < endDate)
            {
                Console.WriteLine($"Generating {currentDate}");
                var impressions = Enumerable.Range(0, Utils.Random(100, 1000))
                    .Select(p => ImpressionEvent.Create(currentDate)).SelectMany(p=>ImpressionEventFlatten.From(p)).ToArray();

                yield return impressions;
                currentDate = currentDate.AddDays(1);
            }
        }
    }
}

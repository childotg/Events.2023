using IngestReports.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IngestReports
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
    }
}

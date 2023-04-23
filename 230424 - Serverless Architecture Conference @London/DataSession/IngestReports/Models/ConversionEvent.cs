using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IngestReports.Models
{
    public class ConversionEvent
    {
        public DateTimeOffset createdAt { get; set; }
        public string country { get; set; }
        public string account { get; set; }
        public string campaign { get; set; }
        public double sellout { get; set; }
        public double cost { get; set; }
        public int conversions { get; set; }

        public static ConversionEvent Create(DateTime? day=null)
        {
            return new ConversionEvent()
            {
                createdAt = Utils.RandomDateTime(day),
                country = Utils.RandomCountry(),
                account = Utils.RandomAccount(),
                campaign = Utils.RandomCampaign(),
                sellout = Utils.RandomSellout(),
                cost = Utils.RandomCost(),
                conversions = Utils.RandomConversions()
            };
        }

    }
}

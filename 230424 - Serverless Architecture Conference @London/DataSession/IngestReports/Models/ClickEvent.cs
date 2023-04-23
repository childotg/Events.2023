using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IngestReports.Models
{
    public class ClickEvent
    {
        public DateTimeOffset createdAt { get; set; }
        public string country { get; set; }
        public string account { get; set; }
        public string campaign { get; set; }
        public double cost { get; set; }
        public int clicks { get; set; }

        public static ClickEvent Create(DateTime? day = null)
        {
            return new ClickEvent()
            {
                createdAt = Utils.RandomDateTime(day),
                country = Utils.RandomCountry(),
                account = Utils.RandomAccount(),
                campaign = Utils.RandomCampaign(),
                cost = Utils.RandomCost(),
                clicks = Utils.RandomClicks()
            };
        }
    }
}

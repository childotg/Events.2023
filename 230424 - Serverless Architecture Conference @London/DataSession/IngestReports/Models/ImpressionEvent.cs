using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IngestReports.Models
{
    public class ImpressionEvent
    {
        public long epoch { get; set; }
        public string country_code { get; set; }
        public string account { get; set; }
        public ImpressionEventItem[] impressions { get; set; }

        public static ImpressionEvent Create(DateTime? day = null)
        {
            return new ImpressionEvent()
            {
                epoch = Utils.RandomDateTime(day).ToUnixTimeSeconds(),
                country_code = Utils.RandomCountry(),
                account = Utils.RandomAccount(),
                impressions = Enumerable.Range(0,Utils.Random(2,30))
                    .Select(p=>new ImpressionEventItem
                    {
                        campaign_id=Utils.RandomCampaign(),
                        cost=Utils.RandomCost(),
                        creativity_id=Utils.RandomCreativity(),
                        impression_count=Utils.RandomClicks()*100
                    }).ToArray()
            };
        }

    }

    public class ImpressionEventFlatten
    {
        public long epoch { get; set; }
        public string country_code { get; set; }
        public string account { get; set; }
        public string campaign_id { get; set; }
        public string creativity_id { get; set; }
        public int impression_count { get; set; }
        public double cost { get; set; }

        public static IEnumerable<ImpressionEventFlatten> From(ImpressionEvent source)
        {
            return source.impressions.Select(p => new ImpressionEventFlatten()
            {
                account=source.account,
                epoch=source.epoch,
                country_code=source.country_code,
                campaign_id=p.campaign_id,
                cost=p.cost,
                creativity_id=p.creativity_id,
                impression_count=p.impression_count
            });
        }
    }

    public class ImpressionEventItem
    {
        public string campaign_id { get; set; }
        public string creativity_id { get; set; }
        public int impression_count { get; set; }
        public double cost { get; set; }
    }
}

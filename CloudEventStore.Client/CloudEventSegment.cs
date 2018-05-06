using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudEventStore
{
    public class CloudEventSegment
    {
        [JsonProperty("results")]
        public List<CloudEvent> Results { get; set; }

        [JsonProperty("next")]
        public Uri Next { get; set; }
    }
}

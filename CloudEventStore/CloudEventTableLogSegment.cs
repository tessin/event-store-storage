using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudEventStore
{
    public class CloudEventTableLogSegment
    {
        public List<CloudEventLogPositionLength> Results { get; }

        public CloudEventTableLogSegment(List<CloudEventLogPositionLength> results, object p)
        {
            this.Results = results;
        }
    }
}

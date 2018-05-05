using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudEventStore
{
    public struct CloudEventLogSegment2
    {
        public List<CloudEvent> Results { get; }
        public CloudEventLogPosition Next { get; }

        public CloudEventLogSegment2(List<CloudEvent> results, CloudEventLogPosition next)
        {
            this.Results = results;
            this.Next = next;
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudEventStore
{
    public struct CloudEventLogSegment
    {
        public List<CloudEvent> Results { get; }
        public CloudEventLogPosition Next { get; }

        public CloudEventLogSegment(List<CloudEvent> results, CloudEventLogPosition next)
        {
            this.Results = results;
            this.Next = next;
        }
    }
}

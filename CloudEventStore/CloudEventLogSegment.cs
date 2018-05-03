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
        public long ContinuationToken { get; }

        public CloudEventLogSegment(List<CloudEvent> results, long value)
        {
            Results = results;
            ContinuationToken = value;
        }
    }
}

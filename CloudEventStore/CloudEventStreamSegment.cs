using System.Collections.Generic;

namespace CloudEventStore
{
    public struct CloudEventStreamSegment
    {
        public List<CloudEvent> Results { get; }
        public CloudEventStreamSequence Next { get; }

        public CloudEventStreamSegment(List<CloudEvent> results, CloudEventStreamSequence next)
        {
            this.Results = results;
            this.Next = next;
        }
    }
}

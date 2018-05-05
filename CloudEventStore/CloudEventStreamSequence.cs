using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudEventStore
{
    public struct CloudEventStreamSequence
    {
        public readonly Guid StreamId;
        public readonly int SequenceNumber;

        public CloudEventStreamSequence(Guid streamId, int sequenceNumber)
        {
            StreamId = streamId;
            SequenceNumber = sequenceNumber;
        }
    }
}

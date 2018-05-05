using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudEventStore
{
    public struct CloudEventHeader
    {
        public readonly Guid StreamId;
        public readonly int SequenceNumber;
        public readonly long Offset;
        public readonly long Size;

        public CloudEventHeader(Guid streamId, int sequenceNumber, long offset, long size)
        {
            this.StreamId = streamId;
            this.SequenceNumber = sequenceNumber;
            this.Offset = offset;
            this.Size = size;
        }
    }
}

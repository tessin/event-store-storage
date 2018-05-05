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
        public readonly long Position;
        public readonly long Length;

        public CloudEventHeader(Guid streamId, int sequenceNumber, long position, long length)
        {
            this.StreamId = streamId;
            this.SequenceNumber = sequenceNumber;
            this.Position = position;
            this.Length = length;
        }
    }
}

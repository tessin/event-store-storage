using System;

namespace CloudEventStore
{
    public class UncommittedEvent
    {
        public Guid StreamId { get; set; }
        public int SequenceNumber { get; set; }
        public Guid TypeId { get; set; }
        public DateTimeOffset Created { get; set; }
        public ArraySegment<byte> Payload { get; set; }
    }
}

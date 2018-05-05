using CloudEventStore.Internal;
using System;
using System.IO;

namespace CloudEventStore
{
    public class CloudEvent
    {
        public CloudEventLogPosition Id { get; }
        public Guid StreamId { get; }
        public int SequenceNumber { get; }
        public Guid TypeId { get; }
        public DateTimeOffset Created { get; }
        public ArraySegment<byte> Payload { get; }

        public CloudEvent(CloudEventLogPosition id, ArraySegment<byte> data)
        {
            Id = id;

            var uuid = new byte[16];

            var stream = new MemoryStream(data.Array, data.Offset, data.Count, false, true);

            stream.Read(uuid, 0, 16);
            StreamId = new Guid(uuid);

            SequenceNumber = (int)stream.ReadVarInt63();

            stream.Read(uuid, 0, 16);
            TypeId = new Guid(uuid);

            Created = DateTimeOffset.FromUnixTimeMilliseconds(stream.ReadVarInt63());

            var payloadSize = stream.ReadVarInt63();
            Payload = new ArraySegment<byte>(data.Array, data.Offset + (int)stream.Position, (int)payloadSize);
        }
    }
}

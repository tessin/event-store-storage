using CloudEventStore.Internal;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudEventStore
{
    public class CloudEvent
    {
        public CloudEventLogSequenceNumber Id { get; }

        public Guid StreamId { get; }
        public int SequenceNumber { get; }
        public Guid TypeId { get; }
        public DateTimeOffset Created { get; }
        public ArraySegment<byte> Payload { get; }

        public CloudEvent(CloudEventLogSequenceNumber lsn, byte[] data)
        {
            Id = lsn;

            var uuid = new byte[16];

            var stream = new MemoryStream(data);

            stream.Read(uuid, 0, 16);
            StreamId = new Guid(uuid);

            SequenceNumber = (int)stream.ReadVarInt63();

            stream.Read(uuid, 0, 16);
            TypeId = new Guid(uuid);

            Created = DateTimeOffset.FromUnixTimeMilliseconds(stream.ReadVarInt63());

            var payloadSize = stream.ReadVarInt63();
            Payload = new ArraySegment<byte>(data, (int)stream.Position, (int)payloadSize);
        }
    }
}

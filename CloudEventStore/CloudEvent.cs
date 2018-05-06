using CloudEventStore.Internal;
using Newtonsoft.Json;
using System;
using System.IO;

namespace CloudEventStore
{
    class CloudEventJsonConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            throw new NotImplementedException();
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var e = (CloudEvent)value;

            writer.WriteStartObject();

            writer.WritePropertyName("id");
            writer.WriteValue(e.Id.Value);

            writer.WritePropertyName("sid");
            writer.WriteValue(e.StreamId);

            writer.WritePropertyName("n");
            writer.WriteValue(e.SequenceNumber);

            writer.WritePropertyName("tid");
            writer.WriteValue(e.TypeId);

            writer.WritePropertyName("t");
            writer.WriteValue(e.Created);

            writer.WritePropertyName("b");
            writer.WriteValue(Convert.ToBase64String(e.Payload.Array, e.Payload.Offset, e.Payload.Count));

            writer.WriteEndObject();
        }
    }

    [JsonConverter(typeof(CloudEventJsonConverter))]
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

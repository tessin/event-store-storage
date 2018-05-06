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
            if (reader.TokenType == JsonToken.StartObject)
            {
                var e = new CloudEvent();

                while (reader.Read())
                {
                    if (reader.TokenType == JsonToken.EndObject)
                    {
                        break;
                    }
                    var propertyName = (string)reader.Value;
                    if (reader.Read())
                    {
                        switch (propertyName)
                        {
                            case "id": e.Id = serializer.Deserialize<long>(reader); break;
                            case "sid": e.StreamId = serializer.Deserialize<Guid>(reader); break;
                            case "n": e.SequenceNumber = serializer.Deserialize<int>(reader); break;
                            case "tid": e.TypeId = serializer.Deserialize<Guid>(reader); break;
                            case "t": e.Created = serializer.Deserialize<DateTimeOffset>(reader); break;
                            case "b": e.Payload = new ArraySegment<byte>(serializer.Deserialize<byte[]>(reader) ?? new byte[0]); break;

                            default:
                                reader.Skip();
                                break;
                        }
                    }
                }

                return e;
            }
            return null;
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var e = (CloudEvent)value;

            writer.WriteStartObject();

            writer.WritePropertyName("id");
            writer.WriteValue(e.Id);

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
        public long Id { get; set; }
        public Guid StreamId { get; set; }
        public int SequenceNumber { get; set; }
        public Guid TypeId { get; set; }
        public DateTimeOffset Created { get; set; }
        public ArraySegment<byte> Payload { get; set; }
    }
}

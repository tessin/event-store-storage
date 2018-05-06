using Newtonsoft.Json;
using System;

namespace CloudEventStore
{
    public class UncommittedEventJsonConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            throw new NotImplementedException();
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader.TokenType == JsonToken.StartObject)
            {
                var e = new UncommittedEvent();

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
            throw new NotImplementedException();
        }
    }

    [JsonConverter(typeof(UncommittedEventJsonConverter))]
    public class UncommittedEvent
    {
        public Guid StreamId { get; set; }
        public int SequenceNumber { get; set; }
        public Guid TypeId { get; set; }
        public DateTimeOffset Created { get; set; }
        public ArraySegment<byte> Payload { get; set; }
    }
}

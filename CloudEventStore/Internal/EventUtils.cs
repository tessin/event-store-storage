using System;
using System.IO;

namespace CloudEventStore.Internal
{
    static class EventUtils
    {
        public static void WriteTo(this UncommittedEvent e, Stream outputStream)
        {
            outputStream.Write(e.StreamId.ToByteArray(), 0, 16);
            outputStream.WriteVarInt63(e.SequenceNumber);
            outputStream.Write(e.TypeId.ToByteArray(), 0, 16);
            outputStream.WriteVarInt63(e.Created.ToUnixTimeMilliseconds());
            outputStream.WriteVarInt63(e.Payload.Count);
            if (0 < e.Payload.Count)
            {
                outputStream.Write(e.Payload.Array, e.Payload.Offset, e.Payload.Count);
            }
        }

        public static void ReadFrom(this CloudEvent e, MemoryStream inputStream, ref byte[] scratch)
        {
            if (scratch == null || scratch.Length < 16)
            {
                scratch = new byte[16];
            }

            inputStream.Read(scratch, 0, 16);
            e.StreamId = new Guid(scratch);

            e.SequenceNumber = (int)inputStream.ReadVarInt63();

            inputStream.Read(scratch, 0, 16);
            e.TypeId = new Guid(scratch);

            e.Created = DateTimeOffset.FromUnixTimeMilliseconds(inputStream.ReadVarInt63());

            var payloadSize = inputStream.ReadVarInt63();

            if (inputStream.TryGetBuffer(out var buffer)) // if buffer is publicly visible
            {
                e.Payload = new ArraySegment<byte>(buffer.Array, buffer.Offset + (int)inputStream.Position, (int)payloadSize);
            }
            else
            {
                var payload = new byte[payloadSize];
                inputStream.Read(payload, 0, payload.Length);
                e.Payload = new ArraySegment<byte>(payload);
            }
        }
    }
}

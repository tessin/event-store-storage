using CloudEventStore.Internal;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace CloudEventStore
{
    public class CloudEventStore2 : ICloudEventStore
    {
        private readonly ICloudEventBlobClient _blobClient;
        private readonly ICloudEventTableClient _tableClient;

        public CloudEventStore2(ICloudEventBlobClient blobClient, ICloudEventTableClient tableClient)
        {
            _blobClient = blobClient;
            _tableClient = tableClient;
        }

        public async Task<List<long>> AppendAsync(IEnumerable<UncommittedEvent> uncommitted, CancellationToken cancellationToken = default(CancellationToken))
        {
            var block = new MemoryStream();
            var headers = new List<CloudEventHeader>();

            var scratch = new MemoryStream();
            foreach (var e in uncommitted)
            {
                if (!(0 < e.SequenceNumber))
                {
                    throw new ArgumentException("uncommited event sequence number must be non-zero positive integer", nameof(uncommitted));
                }

                scratch.SetLength(0);

                scratch.Write(e.StreamId.ToByteArray(), 0, 16);
                scratch.WriteVarInt63(e.SequenceNumber);
                scratch.Write(e.TypeId.ToByteArray(), 0, 16);
                scratch.WriteVarInt63(e.Created.ToUnixTimeMilliseconds());
                scratch.WriteVarInt63(e.Payload.Count);
                if (0 < e.Payload.Count)
                {
                    scratch.Write(e.Payload.Array, e.Payload.Offset, e.Payload.Count);
                }

                var pos = block.Position;

                scratch.Position = 0;
                scratch.CopyTo(block);

                headers.Add(new CloudEventHeader(e.StreamId, e.SequenceNumber, pos, scratch.Length));
            }

            var committed = new List<long>(headers.Count);

            var lsn = await _blobClient.AppendAsync(block, cancellationToken);

            if (await _tableClient.CommitAsync(lsn, headers, cancellationToken))
            {
                foreach (var h in headers)
                {
                    committed.Add(new CloudEventLogSequenceNumber(lsn.LogNumber, lsn.SequenceNumber + h.Offset).Value);
                }
            }

            return committed;
        }
    }
}

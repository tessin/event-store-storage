using AppendTransactionLogBlob.Internal;
using AppendTransactionLogBlob.Metrics;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Tessin.Statistics;

namespace AppendTransactionLogBlob
{
    public class CloudEventStore
    {
        private readonly CloudEventStorage _storage;
        private readonly BucketManager _stat;

        public CloudEventStore(CloudEventStorage storage, BucketManager stat = null)
        {
            _storage = storage;
            _stat = stat;
        }

        public async Task AppendAsync(IEnumerable<UncomittedEvent> uncomitted)
        {
            var t0 = DateTime.UtcNow;

            var w = new MemoryStream();
            var scratch = new MemoryStream();
            var idx = new MemoryStream();

            var headers = new List<CloudEventHeader>();
            foreach (var e in uncomitted)
            {
                if (!(0 < e.SequenceNumber))
                {
                    throw new ArgumentException("uncommited event sequence number must be positive integer, greater than zero", nameof(uncomitted));
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

                var pos = w.Position;

                w.WriteVarInt63(scratch.Position); // size of envelope incl. payload, in bytes

                scratch.Position = 0;
                scratch.CopyTo(w);

                var size = w.Position - pos;
                idx.WriteVarInt63(size); // size of header, envelope incl. payload, in bytes

                headers.Add(new CloudEventHeader(e.StreamId, e.SequenceNumber, pos));
            }

            int n = 0;
            _Commit:
            var commitBlob = await _storage.GetLogBlobNextFromCacheAsync();

            long log = CloudEventBlobName.LogFromName(commitBlob.Name);

            w.Position = 0;

            var blockLength = w.Length;

            var sw = Stopwatch.StartNew();

            long blockOffset;

            try
            {
                blockOffset = await commitBlob.AppendBlockAsync(w);
            }
            catch (StorageException ex) when (ex.RequestInformation.HttpStatusCode == 409 && ex.RequestInformation.ExtendedErrorInformation.ErrorCode == "BlockCountExceedsLimit")
            {
                if (n < 5)
                {
                    _storage.ClearLogBlobNextFromCache();
                    n++;
                    goto _Commit;
                }
#if DEBUG
                Debugger.Launch();
#endif
                throw;
            }

            sw.Stop();

            var appendBlockElapsed = sw.Elapsed;

            var index = await _storage._index;

            var commitTran = new TableBatchOperation();

            foreach (var header in headers)
            {
                commitTran.Insert(new CloudEventStreamEntity(header.StreamId, header.SequenceNumber, new CloudEventLogSequenceNumber(log, blockOffset + header.Offset)), echoContent: false);
            }

            commitTran.Insert(new CloudEventTransactionLogEntity(new CloudEventLogSequenceNumber(log, blockOffset), new CloudEventLogSequenceNumber(log, blockOffset + blockLength), idx.ToArray()), echoContent: false);

            sw.Restart();

            await index.ExecuteBatchAsync(commitTran);

            sw.Stop();

            var executeBatchElapsed = sw.Elapsed;

            _stat?.SubmitAppend(t0, appendBlockElapsed, executeBatchElapsed);
        }

        public async Task<CloudEventLogSegment> GetEventLogSegmentedAsync(long minLsn, long maxLsn = long.MaxValue, int maxCount = 1000)
        {
            var context = new CloudEventStoreContext(_storage);

            var ct = new CloudEventLogSequenceNumber(minLsn);

            var results = new List<CloudEvent>();

            while ((ct.Value < maxLsn) & (results.Count < maxCount))
            {
                if (await context.SeekAsync(ct))
                {
                    using (var reader = await context.OpenReadAsync(ct))
                    {
                        while (await reader.ReadAsync())
                        {
                            results.Add(reader.Current);

                            if (!(results.Count < maxCount))
                            {
                                break;
                            }
                        }
                        ct = reader.Position;
                    }
                }
                else
                {
                    break;
                }
            }

            return new CloudEventLogSegment(results, ct.Value);
        }
    }
}

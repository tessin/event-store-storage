﻿using CloudEventStore.Internal;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CloudEventStore
{
    public class CloudEventStore : ICloudEventStoreClient
    {
        private readonly ICloudEventBlobClient _blobClient;
        private readonly ICloudEventTableClient _tableClient;

        public CloudEventStore(ICloudEventBlobClient blobClient, ICloudEventTableClient tableClient)
        {
            _blobClient = blobClient;
            _tableClient = tableClient;
        }

        public async Task<List<long>> AppendAsync(IEnumerable<UncommittedEvent> uncommitted, CancellationToken cancellationToken = default(CancellationToken))
        {
            var count = uncommitted.Count();

            if (!((1 <= count) & (count <= 99))) throw new ArgumentOutOfRangeException(nameof(uncommitted), "uncommitted batch size must be between 1 and 99");

            var block = new MemoryStream();
            var headers = new List<CloudEventHeader>(count);

            var scratch = new MemoryStream();
            foreach (var e in uncommitted)
            {
                if (!(0 < e.SequenceNumber))
                {
                    throw new ArgumentException("uncommited event sequence number must be non-zero positive integer", nameof(uncommitted));
                }

                scratch.SetLength(0);

                e.WriteTo(scratch);

                var pos = block.Position;

                scratch.Position = 0;
                scratch.CopyTo(block);

                headers.Add(new CloudEventHeader(e.StreamId, e.SequenceNumber, pos, scratch.Length));
            }

            var committed = new List<long>(count);

            block.Position = 0;

            var lsn = await _blobClient.AppendAsync(block, cancellationToken);

            if (await _tableClient.CommitAsync(lsn, headers, cancellationToken))
            {
                foreach (var h in headers)
                {
                    committed.Add(new CloudEventLogPosition(lsn.Log, lsn.Position + h.Position).Value);
                }
            }

            return committed;
        }

        public async Task<CloudEventLogSegment> GetLogSegmentedAsync(CloudEventLogPosition next, int takeCount = 1000, CancellationToken cancellationToken = default(CancellationToken))
        {
            var results1 = await _tableClient.GetLogSegmentedAsync(next, takeCount, cancellationToken);

            var results2 = await _blobClient.FetchAsync(results1.Results);

            var results3 = new List<CloudEvent>();

            byte[] scratch = null;
            foreach (var item in results2)
            {
                var e = new CloudEvent();
                e.Id = new CloudEventLogPosition(item.Log, item.Position).Value;
                e.ReadFrom(item.Data.GetStream(), ref scratch);
                results3.Add(e);
            }

            CloudEventLogPosition next2 = next;

            if (0 < results2.Count)
            {
                var lst = results2[results2.Count - 1];

                next2 = new CloudEventLogPosition(lst.Log, lst.Position + lst.Data.Count);
            }

            return new CloudEventLogSegment(results3, next2);
        }

        public async Task<CloudEventStreamSegment> GetStreamSegmentedAsync(CloudEventStreamSequence next, int takeCount = 1000, CancellationToken cancellationToken = default(CancellationToken))
        {
            var results1 = await _tableClient.GetStreamSegmentedAsync(next, takeCount, cancellationToken);

            var results2 = await _blobClient.FetchAsync(results1.Results);

            var results3 = new List<CloudEvent>();

            byte[] scratch = null;
            foreach (var item in results2)
            {
                var e = new CloudEvent();
                e.Id = new CloudEventLogPosition(item.Log, item.Position).Value;
                e.ReadFrom(item.Data.GetStream(), ref scratch);
                results3.Add(e);
            }

            CloudEventStreamSequence next2 = next;

            if (0 < results3.Count)
            {
                next2 = new CloudEventStreamSequence(next.StreamId, results3[results3.Count - 1].SequenceNumber + 1);
            }

            return new CloudEventStreamSegment(results3, next2);
        }
    }
}

using CloudEventStore.Internal;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CloudEventStore
{
    public class CloudEventIndexClientConfiguration
    {
        public string TableName { get; set; } = "EventStore";
    }

    public class CloudEventTableClient : ICloudEventTableClient
    {
        public CloudEventIndexClientConfiguration Configuration { get; } = new CloudEventIndexClientConfiguration();

        private readonly AsyncLazy<CloudTable> _table;

        public CloudEventTableClient(CloudStorageAccount storageAccount)
        {
            var tableClient = storageAccount.CreateCloudTableClient();

            tableClient.DefaultRequestOptions.PayloadFormat = TablePayloadFormat.JsonNoMetadata;

            tableClient.DefaultRequestOptions.PropertyResolver = (a, b, c, d) =>
            {
                switch (c)
                {
                    case "L": return EdmType.Int32;
                    case "P": return EdmType.Int64;
                    case "S": return EdmType.Int32;
                    case "T": return EdmType.Binary;
                }
                return EdmType.String;
            };

            _table = Async.Lazy(async () =>
            {
                var table = tableClient.GetTableReference(Configuration.TableName);
                await table.CreateIfNotExistsAsync();
                return table;
            });
        }

        public async Task<bool> CommitAsync(CloudEventLogPosition lsnStart, IEnumerable<CloudEventHeader> headers, CancellationToken cancellationToken = default(CancellationToken))
        {
            var b = new TableBatchOperation();

            var idx = new MemoryStream();

            long p = 0;
            foreach (var h in headers)
            {
                var lsn = new CloudEventLogPosition(lsnStart.Log, lsnStart.Position + p);
                b.Insert(new CloudEventStreamEntity(h.StreamId, h.SequenceNumber, lsn, (int)h.Length), echoContent: false);
                idx.WriteVarInt63(h.Length);
                p += h.Length;
            }

            var lsnEnd = new CloudEventLogPosition(lsnStart.Log, lsnStart.Position + p);
            b.Insert(new CloudEventTransactionLogEntity(lsnStart, lsnEnd, idx.ToArray()), echoContent: false);

            var table = await _table;

            try
            {
                // commit!
                await table.ExecuteBatchAsync(b, cancellationToken);
                return true;
            }
            catch (StorageException ex) when (ex.RequestInformation.HttpStatusCode == 409)
            {
                return false; // conflict!
            }
        }

        public async Task<CloudEventLogPositionLengthSegment> GetLogSegmentedAsync(CloudEventLogPosition next, int takeCount = 1000, CancellationToken cancellationToken = default(CancellationToken))
        {
            var table = await _table;

            var q = new TableQuery<CloudEventTransactionLogEntity>
            {
                FilterString =
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, CloudEventTransaction.PartitionKey),
                        TableOperators.And,
                        TableQuery.CombineFilters(
                            TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, CloudEventTransaction.RowKeyTransactionPrefix + CloudEventLogPosition.MinValue),
                            TableOperators.And,
                            TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, CloudEventTransaction.RowKeyTransactionPrefix + CloudEventLogPosition.MaxValue)
                        )
                    ),
                TakeCount = takeCount
            };

            var continuationToken = new TableContinuationToken
            {
                NextPartitionKey = CloudEventTransaction.PartitionKey,
                NextRowKey = CloudEventTransaction.RowKeyTransactionPrefix + next
            };

            var querySegment = await table.ExecuteQuerySegmentedAsync(q, continuationToken);

            var results2 = new List<CloudEventLogPositionLength>();

            foreach (var result in querySegment.Results)
            {
                var idx = new MemoryStream(result.Lengths);

                var p = 0L;
                while (idx.Position < idx.Length)
                {
                    // todo: skip

                    var size = idx.ReadVarInt63();
                    results2.Add(new CloudEventLogPositionLength(result.Log, result.Position + p, (int)size));
                    p += size;
                }

                next = result.GetEnd(); // todo: what if location is past max block count?
            }

            return new CloudEventLogPositionLengthSegment(results2, querySegment.ContinuationToken);
        }

        public async Task<CloudEventLogPositionLengthSegment> GetStreamSegmentedAsync(CloudEventStreamSequence next, int takeCount, CancellationToken cancellationToken)
        {
            var table = await _table;

            // min/max
            var streamId32 = next.StreamId.ToByteArray().Encode32();

            var min = CloudEventTransaction.RowKeyStreamPrefix + streamId32 + "-" + ((long)next.SequenceNumber).ToFixed(10);
            var max = CloudEventTransaction.RowKeyStreamPrefix + streamId32 + "-" + ((long)(next.SequenceNumber + takeCount)).ToFixed(10);

            var q = new TableQuery<CloudEventStreamEntity>
            {
                FilterString =
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, CloudEventTransaction.PartitionKey),
                        TableOperators.And,
                        TableQuery.CombineFilters(
                            TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, min),
                            TableOperators.And,
                            TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, max)
                        )
                    ),
                TakeCount = takeCount
            };

            var querySegment = await table.ExecuteQuerySegmentedAsync(q, null);

            var results2 = new List<CloudEventLogPositionLength>();

            foreach (var item in querySegment.Results)
            {
                results2.Add(new CloudEventLogPositionLength(item.Log, item.Position, item.Length));
            }

            return new CloudEventLogPositionLengthSegment(results2, querySegment.ContinuationToken);
        }
    }
}

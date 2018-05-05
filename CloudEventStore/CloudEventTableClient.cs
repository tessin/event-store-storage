using CloudEventStore.Internal;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;
using System.IO;
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

            _table = Async.Lazy(async () =>
            {
                var table = tableClient.GetTableReference(Configuration.TableName);
                await table.CreateIfNotExistsAsync();
                return table;
            });
        }

        public async Task<bool> CommitAsync(CloudEventLogSequenceNumber lsnStart, IEnumerable<CloudEventHeader> headers, CancellationToken cancellationToken = default(CancellationToken))
        {
            var b = new TableBatchOperation();

            var idx = new MemoryStream();

            long p = 0;
            foreach (var h in headers)
            {
                var lsn = new CloudEventLogSequenceNumber(lsnStart.LogNumber, lsnStart.SequenceNumber + p);
                b.Insert(new CloudEventStreamEntity(h.StreamId, h.SequenceNumber, lsn), echoContent: false);
                idx.WriteVarInt63(p);
                p += h.Size;
            }

            idx.WriteVarInt63(p);

            var lsnEnd = new CloudEventLogSequenceNumber(lsnStart.LogNumber, lsnStart.SequenceNumber + p);
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
    }
}

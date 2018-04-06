using AppendTransactionLogBlob;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace AppendBlobEventStore
{
    static class CloudAppendBlobExtensions
    {
        public static async Task<bool> CreateIfNotExistsAsync(this CloudAppendBlob appendBlob)
        {
            try
            {
                await appendBlob.CreateOrReplaceAsync(AccessCondition.GenerateIfNotExistsCondition(), null, null);
            }
            catch (StorageException ex) when (ex.RequestInformation.HttpStatusCode == 409)
            {
                return false;
            }
            return true;
        }
    }

    // singelton
    public class EventStoreStorage
    {
        private readonly CloudStorageAccount _account;

        public readonly AsyncLazy<CloudBlobContainer> _store;
        public readonly AsyncLazy<CloudTable> _index;

        public EventStoreStorage(CloudStorageAccount account, string containerName, string tableName)
        {
            _account = account;

            var blobClient = account.CreateCloudBlobClient();

            _store = new AsyncLazy<CloudBlobContainer>(async () =>
            {
                var store = blobClient.GetContainerReference(containerName);
                await store.CreateIfNotExistsAsync();
                return store;
            });

            var tableClient = account.CreateCloudTableClient();

            _index = new AsyncLazy<CloudTable>(async () =>
            {
                var index = tableClient.GetTableReference(tableName);
                await index.CreateIfNotExistsAsync();
                return index;
            });
        }


        private static string GetAppendBlobName(long id)
        {
            return FormattableString.Invariant($"blob-{id:00000000}");
        }

        public async Task<CloudAppendBlob> GetAppendBlobAsync()
        {
            var store = await _store;

            // this needs to be cached

            var blobs = await store.ListBlobsSegmentedAsync(null);
            if (blobs.Results.Any())
            {
                var blob = (CloudAppendBlob)blobs.Results.Last();
                return blob;
            }
            else
            {
                var blob = store.GetAppendBlobReference(GetAppendBlobName(0));
                await blob.CreateIfNotExistsAsync();
                return blob;
            }
        }

        public async Task<CloudAppendBlob> GetAppendBlobAsync(long id)
        {
            var store = await _store;

            //          4*1024*1024 = 209715200000
            //   log2(209715200000) = 37.6
            //                63-38 = 25
            //                 2^25 = 33554432
            //       log2(33554432) = 7.5
            //       33554432*50000 = 1677721600000
            // log10(1677721600000) = 12.2

            var blob = store.GetAppendBlobReference(GetAppendBlobName(id));
            return blob;
        }
    }

    struct EventHeader
    {
        public readonly Guid streamId;
        public readonly int sequenceNumber;
        public readonly string name;
        public readonly int offset;
        public readonly int size;

        public EventHeader(Guid streamId, int sequenceNumber, string name, int offset, int size)
        {
            this.streamId = streamId;
            this.sequenceNumber = sequenceNumber;
            this.name = name;
            this.offset = offset;
            this.size = size; // must be less than 4 MiB
        }
    }

    class EventMetadataEntity : TableEntity
    {
        public EventMetadataEntity()
        {
        }

        public EventMetadataEntity(long offset, EventHeader metadata)
        {
            PartitionKey = "committed";
            RowKey = FormattableString.Invariant($"s-{metadata.streamId}-{metadata.sequenceNumber:0000000000}");
            Name = metadata.name; // append blob ID?
            Offset = offset + metadata.offset;
        }

        public string Name { get; set; }
        public long Offset { get; set; }
    }

    class EventTransactionEntity : TableEntity
    {
        public EventTransactionEntity()
        {
        }

        public EventTransactionEntity(Guid id)
        {
            PartitionKey = "committed";
            RowKey = FormattableString.Invariant($"t-{id}");
        }
    }

    public class EventStore
    {
        private readonly EventStoreStorage _storage;

        public EventStore(EventStoreStorage storage)
        {
            _storage = storage;
        }

        public async Task AppendAsync(IEnumerable<UncomittedEvent> uncomitted)
        {
            var blob = await _storage.GetAppendBlobAsync();

            var mem1 = new MemoryStream();
            var textWriter = new StreamWriter(mem1, Encoding.UTF8);
            var jsonWriter = new JsonTextWriter(textWriter);

            var headers = new List<EventHeader>();

            var serializer = new JsonSerializer();
            foreach (var e in uncomitted)
            {
                var p0 = mem1.Position;
                serializer.Serialize(jsonWriter, uncomitted);
                textWriter.Flush(); // flush is necessary between calls to serialize to ensure correct position and size in stream
                headers.Add(new EventHeader(e.StreamId, e.SequenceNumber, blob.Name, (int)p0, (int)(mem1.Position - p0))); // must be less than 4 MiB
            }

            var tranId = Guid.NewGuid();

            var headerBuffer = new byte[16 + 4 + 4 * headers.Count + 4];

            var w = new BinaryWriter(new MemoryStream(headerBuffer, true));

            w.Write(tranId.ToByteArray()); // 16

            w.Write(headers.Count); // 4

            foreach (var header in headers) // 4 * headers.Count
            {
                w.Write(header.offset);
            }

            w.Write(headers[headers.Count - 1].offset + headers[headers.Count - 1].size); // 4

            mem1.Position = 0;

            var offset = await blob.AppendBlockAsync(new MemoryStream(headerBuffer).Concat(mem1));

            var index = await _storage._index;

            var b = new TableBatchOperation();

            foreach (var metadata in headers)
            {
                b.Add(TableOperation.Insert(new EventMetadataEntity(offset, metadata)));
            }

            b.Add(TableOperation.Insert(new EventTransactionEntity(tranId)));

            await index.ExecuteBatchAsync(b); // comitted
        }

        public async Task<List<CloudEvent>> GetEventSegmented(long min, long max = long.MaxValue, int maxCount = 1000)
        {
            // get the input blob
            // get the output blob

            var blob = await _storage.GetAppendBlobAsync(min);
            var index = await _storage._index;

            var inputStream = await blob.OpenReadAsync();

            var reader = new BinaryReader(inputStream);

            var tranId = reader.ReadBytes(16);

            var commit = await index.ExecuteAsync(TableOperation.Retrieve("committed", "t-" + new Guid(tranId)));
            if (commit.HttpStatusCode == 404)
            {
                // not committed, ignore
            }

            // serialize, where?

            return null;
        }
    }

    public class CloudEvent
    {
    }

    public class UncomittedEvent
    {
        public Guid StreamId { get; set; }
        public int SequenceNumber { get; set; }
        public Guid TypeId { get; set; }
        public byte[] Payload { get; set; }
        public DateTimeOffset Created { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var storageAccount = CloudStorageAccount.Parse(System.Configuration.ConfigurationManager.AppSettings["storage-account"]);

            MainAsync(storageAccount).GetAwaiter().GetResult();
            return;

            var storage = new EventStoreStorage(storageAccount, "event-store", "EventStoreIndex");
            var store = new EventStore(storage);
            store.AppendAsync(new[] { new UncomittedEvent { StreamId = Guid.NewGuid(), SequenceNumber = 1, Created = DateTimeOffset.UtcNow } }).GetAwaiter().GetResult();
        }

        private static async Task MainAsync(CloudStorageAccount storageAccount)
        {
            var blobClient = storageAccount.CreateCloudBlobClient();

            var test = blobClient.GetContainerReference("test");
            await test.CreateIfNotExistsAsync();

            var blob = test.GetAppendBlobReference("max-block-size");
            await blob.CreateIfNotExistsAsync();

            var block = new byte[4 * 1024 * 1024];

            await blob.AppendBlockAsync(new MemoryStream(block, 0, block.Length, false));
        }
    }

    public class AsyncLazy<T> : Lazy<Task<T>>
    {
        public AsyncLazy(Func<Task<T>> taskFactory)
            : base(() => Task.Factory.StartNew(() => taskFactory()).Unwrap())
        {
        }

        public System.Runtime.CompilerServices.TaskAwaiter<T> GetAwaiter()
        {
            return Value.GetAwaiter();
        }
    }
}

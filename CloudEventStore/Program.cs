using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.IO;
using System.Threading.Tasks;

namespace CloudEventStore
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

    struct CloudEventHeader
    {
        public readonly Guid StreamId;
        public readonly int SequenceNumber;
        public readonly long Offset;

        public CloudEventHeader(Guid streamId, int sequenceNumber, long offset)
        {
            this.StreamId = streamId;
            this.SequenceNumber = sequenceNumber;
            this.Offset = offset;
        }
    }

    public class UncomittedEvent
    {
        public Guid StreamId { get; set; }
        public int SequenceNumber { get; set; }
        public Guid TypeId { get; set; }
        public DateTimeOffset Created { get; set; }
        public ArraySegment<byte> Payload { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var storageAccount = CloudStorageAccount.DevelopmentStorageAccount;

            MainAsync(storageAccount).GetAwaiter().GetResult();
            return;

            var storage = new CloudEventStorage(storageAccount, "event-store", "EventStoreIndex");
            var store = new CloudEventStore(storage);
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


}

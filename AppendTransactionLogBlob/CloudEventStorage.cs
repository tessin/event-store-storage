using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace AppendTransactionLogBlob
{
    // singelton
    public class CloudEventStorage
    {
        public int ConnectionLimit { get; set; } = 250;
        public int AppendBlobBlockCountMax { get; set; } = 50000;

        private readonly CloudStorageAccount _account;

        public readonly AsyncLazy<CloudBlobContainer> _store;
        public readonly AsyncLazy<CloudTable> _index;

        public CloudEventStorage(CloudStorageAccount account, string containerName, string tableName)
        {
            _account = account;

            var blobClient = account.CreateCloudBlobClient();

            blobClient.DefaultRequestOptions.StoreBlobContentMD5 = true;

            _store = new AsyncLazy<CloudBlobContainer>(async () =>
            {
                var store = blobClient.GetContainerReference(containerName);
                await store.CreateIfNotExistsAsync();

                var servicePoint = ServicePointManager.FindServicePoint(store.Uri);
                servicePoint.ConnectionLimit = ConnectionLimit;
                servicePoint.UseNagleAlgorithm = false;

                return store;
            });

            var tableClient = account.CreateCloudTableClient();

            _index = new AsyncLazy<CloudTable>(async () =>
            {
                var index = tableClient.GetTableReference(tableName);

                tableClient.DefaultRequestOptions.PayloadFormat = TablePayloadFormat.JsonNoMetadata;

                tableClient.DefaultRequestOptions.PropertyResolver = (a, b, c, d) =>
                {
                    switch (c)
                    {
                        case "LSN": return EdmType.Int64;
                        case "IDX": return EdmType.Binary;
                    }
                    return EdmType.String;
                };

                await index.CreateIfNotExistsAsync();

                var servicePoint = ServicePointManager.FindServicePoint(index.Uri);
                servicePoint.ConnectionLimit = ConnectionLimit;
                servicePoint.UseNagleAlgorithm = false;

                return index;
            });
        }

        public async Task<CloudAppendBlob> GetLogBlobNextFromServerAsync()
        {
            var store = await _store;

            var blobs = await store.ListBlobsSegmentedAsync(CloudEventBlobName.LogPrefix, true, BlobListingDetails.None, 1, null, null, null);
            if (blobs.Results.Any())
            {
                var blob = (CloudAppendBlob)blobs.Results.First();
                await blob.FetchAttributesAsync();
                if (blob.Properties.AppendBlobCommittedBlockCount < AppendBlobBlockCountMax) // limit
                {
                    return blob;
                }

                var log = CloudEventBlobName.LogFromName(blob.Name);

                var blob2 = store.GetAppendBlobReference(CloudEventBlobName.Log(new CloudEventLogSequenceNumber(log + 1, 0)));
                await blob2.CreateIfNotExistsAsync();
                return blob2;
            }
            else
            {
                var blob = store.GetAppendBlobReference(CloudEventBlobName.Log(new CloudEventLogSequenceNumber()));
                await blob.CreateIfNotExistsAsync();
                return blob;
            }
        }

        private CloudAppendBlob _logBlob;

        public async Task<CloudAppendBlob> GetLogBlobNextFromCacheAsync()
        {
            if (_logBlob == null || (AppendBlobBlockCountMax <= _logBlob.Properties.AppendBlobCommittedBlockCount))
            {
                _logBlob = await GetLogBlobNextFromServerAsync();
            }
            return _logBlob;
        }

        public void ClearLogBlobNextFromCache()
        {
            _logBlob = null;
        }

        public async Task<CloudAppendBlob> GetLogBlobAsync(CloudEventLogSequenceNumber lsn)
        {
            var store = await _store;

            var blob = store.GetAppendBlobReference(CloudEventBlobName.Log(lsn));
            return blob;
        }
    }
}

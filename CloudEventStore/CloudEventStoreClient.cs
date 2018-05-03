using CloudEventStore.Internal;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CloudEventStore
{
    public class CloudEventStoreClientConfiguration
    {
        public int MaxCommittedBlockCount { get; set; } = 50000; // todo: max blob size?
        public string ContainerName { get; set; } = "event-store";
        public string TableName { get; set; } = "EventStore";
    }

    public class CloudEventStoreClient : ICloudEventStoreClient
    {
        public CloudEventStoreClientConfiguration Configuration { get; } = new CloudEventStoreClientConfiguration();

        private readonly AsyncLazy<CloudBlobContainer> _container;

        public CloudEventStoreClient(CloudStorageAccount storageAccount)
        {
            var blobClient = storageAccount.CreateCloudBlobClient();

            _container = Async.Lazy(async () =>
            {
                var container = blobClient.GetContainerReference(Configuration.ContainerName);
                await container.CreateIfNotExistsAsync();
                return container;
            });
        }

        public static readonly string AppendBlobPrefix = "log-";

        private static string GetAppendBlobName(long logNumber)
        {
            // descending sort order
            var lsn = new CloudEventLogSequenceNumber(CloudEventLogSequenceNumber.MaxValue.LogNumber - logNumber, 0);
            return AppendBlobPrefix + lsn.LogNumberFixed8 + ".dat";
        }

        public static long GetLogNumberFromAppendBlobName(string name)
        {
            return CloudEventLogSequenceNumber.MaxValue.LogNumber - name.FromFixed(AppendBlobPrefix.Length, 8);
        }

        private async Task<string> GetAppendBlobNameFromServerAsync()
        {
            var container = await _container;

            var listSegment = await container.ListBlobsSegmentedAsync(AppendBlobPrefix, true, BlobListingDetails.None, 1, null, null, null);

            var appendBlob = (CloudAppendBlob)listSegment.Results.FirstOrDefault();
            if (appendBlob == null)
            {
                // first!
                var appendBlob2 = container.GetAppendBlobReference(GetAppendBlobName(0));
                await appendBlob2.CreateIfNotExistsAsync();
                return appendBlob2.Name;
            }
            else
            {
                await appendBlob.FetchAttributesAsync();
                if (appendBlob.Properties.AppendBlobCommittedBlockCount.Value < Configuration.MaxCommittedBlockCount)
                {
                    return appendBlob.Name; // use this blob
                }
                else
                {
                    // second!
                    var logNumber = GetLogNumberFromAppendBlobName(appendBlob.Name);
                    var appendBlob2 = container.GetAppendBlobReference(GetAppendBlobName(logNumber + 1));
                    await appendBlob2.CreateIfNotExistsAsync();
                    return appendBlob2.Name;
                }
            }
        }

        private string _cachedAppendBlobName; // todo: data race?

        private async Task<CloudAppendBlob> GetAppendBlobAsync()
        {
            if (_cachedAppendBlobName == null)
            {
                _cachedAppendBlobName = await GetAppendBlobNameFromServerAsync();
            }
            var container = await _container;
            return container.GetAppendBlobReference(_cachedAppendBlobName);
        }

        public async Task<CloudEventLogSequenceNumber> AppendAsync(Stream inputStream, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (!inputStream.CanSeek) throw new ArgumentException("inputStream not seekable", nameof(inputStream));

            var n = 0;
            _GetAppendBlobAsync:

            cancellationToken.ThrowIfCancellationRequested();

            var appendBlob = await GetAppendBlobAsync();

            long sequenceNumber;
            try
            {
                sequenceNumber = await appendBlob.AppendBlockAsync(inputStream);
            }
            catch (StorageException ex) when (ex.RequestInformation.HttpStatusCode == 409 && ex.RequestInformation.ExtendedErrorInformation.ErrorCode == "BlockCountExceedsLimit")
            {
                inputStream.Position = 0;
                n++;
                _cachedAppendBlobName = null; // todo: data race?
                goto _GetAppendBlobAsync;
            }

            if (!(appendBlob.Properties.AppendBlobCommittedBlockCount.Value < Configuration.MaxCommittedBlockCount))
            {
                _cachedAppendBlobName = null; // todo: data race?
            }

            var logNumber = GetLogNumberFromAppendBlobName(appendBlob.Name);
            return new CloudEventLogSequenceNumber(logNumber, sequenceNumber);
        }
    }
}

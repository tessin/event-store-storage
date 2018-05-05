using CloudEventStore.Internal;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CloudEventStore
{
    public class CloudEventBlobClientConfiguration
    {
        public int MaxBlockCount { get; set; } = 50000;
        //public long? MaxBlobSize { get; set; } // todo: ???
        public string ContainerName { get; set; } = "event-store";
    }

    public class CloudEventBlobClient : ICloudEventBlobClient
    {
        public CloudEventBlobClientConfiguration Configuration { get; } = new CloudEventBlobClientConfiguration();

        private readonly AsyncLazy<CloudBlobContainer> _container;

        public CloudEventBlobClient(CloudStorageAccount storageAccount)
        {
            var blobClient = storageAccount.CreateCloudBlobClient();

            _container = Async.Lazy(async () =>
            {
                var container = blobClient.GetContainerReference(Configuration.ContainerName);
                await container.CreateIfNotExistsAsync();
                return container;
            });
        }

        private static readonly string AppendBlobPrefix = "log-";

        private static string GetAppendBlobName(long logNumber)
        {
            // descending sort order
            var lsn = new CloudEventLogSequenceNumber(CloudEventLogSequenceNumber.MaxValue.LogNumber - logNumber, 0);
            return AppendBlobPrefix + lsn.LogNumberFixed8 + ".dat";
        }

        private static long GetLogNumberFromAppendBlobName(string name)
        {
            return CloudEventLogSequenceNumber.MaxValue.LogNumber - name.FromFixed(AppendBlobPrefix.Length, 8);
        }

        private async Task<string> GetAppendBlobNameFromServerAsync(CancellationToken cancellationToken)
        {
            var container = await _container;

            var listSegment = await container.ListBlobsSegmentedAsync(AppendBlobPrefix, true, BlobListingDetails.None, 1, null, null, null, cancellationToken);

            var appendBlob = (CloudAppendBlob)listSegment.Results.FirstOrDefault();
            if (appendBlob == null)
            {
                // first!
                var appendBlob2 = container.GetAppendBlobReference(GetAppendBlobName(0));
                await appendBlob2.CreateIfNotExistsAsync(cancellationToken);
                return appendBlob2.Name;
            }
            else
            {
                await appendBlob.FetchAttributesAsync(cancellationToken);
                if (appendBlob.Properties.AppendBlobCommittedBlockCount.Value < Configuration.MaxBlockCount)
                {
                    return appendBlob.Name; // use this blob
                }
                else
                {
                    // second!
                    var logNumber = GetLogNumberFromAppendBlobName(appendBlob.Name);
                    var appendBlob2 = container.GetAppendBlobReference(GetAppendBlobName(logNumber + 1));
                    await appendBlob2.CreateIfNotExistsAsync(cancellationToken);
                    return appendBlob2.Name;
                }
            }
        }

        private string _cachedAppendBlobName; // todo: data race?

        private async Task<CloudAppendBlob> GetAppendBlobAsync(CancellationToken cancellationToken)
        {
            if (_cachedAppendBlobName == null)
            {
                _cachedAppendBlobName = await GetAppendBlobNameFromServerAsync(cancellationToken);
            }
            var container = await _container;
            return container.GetAppendBlobReference(_cachedAppendBlobName);
        }

        public async Task<CloudEventLogSequenceNumber> AppendAsync(Stream block, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (!block.CanSeek) throw new ArgumentException("block stream not seekable", nameof(block));

            var n = 0;
            _GetAppendBlobAsync:

            var appendBlob = await GetAppendBlobAsync(cancellationToken);

            long sequenceNumber;
            try
            {
                sequenceNumber = await AppendInternalAsync(appendBlob, block, cancellationToken);
            }
            catch (StorageException ex) when (ex.RequestInformation.HttpStatusCode == 409)
            {
                // the specific RequestInformation.ExtendedErrorInformation 
                // doesn't appear to be available with the older API version
                // ideally we only intent to handle "BlockCountExceedsLimit"
                // but this will have to do

                block.Position = 0;
                n++;
                _cachedAppendBlobName = null; // todo: data race?
                goto _GetAppendBlobAsync;
            }

            var logNumber = GetLogNumberFromAppendBlobName(appendBlob.Name);
            return new CloudEventLogSequenceNumber(logNumber, sequenceNumber);
        }

        private async Task<long> AppendInternalAsync(CloudAppendBlob appendBlob, Stream block, CancellationToken cancellationToken)
        {
            // this exist to be able to impose arbitrary limits for testing purposes

            var sequenceNumber = await appendBlob.AppendBlockAsync(block, null, cancellationToken);

            if (!(appendBlob.Properties.AppendBlobCommittedBlockCount.Value < Configuration.MaxBlockCount))
            {
                // ignore this block

                throw new StorageException(new RequestResult { HttpStatusCode = 409 }, null, null);
            }

            return sequenceNumber;
        }
    }
}

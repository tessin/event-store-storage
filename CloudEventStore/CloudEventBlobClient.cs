using CloudEventStore.Internal;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
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

            blobClient.DefaultRequestOptions.StoreBlobContentMD5 = true;

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
            var lsn = new CloudEventLogPosition(CloudEventLogPosition.MaxValue.Log - logNumber, 0);
            return AppendBlobPrefix + lsn.LogFixed8 + ".dat";
        }

        private static long GetLogNumberFromAppendBlobName(string name)
        {
            return CloudEventLogPosition.MaxValue.Log - name.FromFixed(AppendBlobPrefix.Length, 8);
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

        public async Task<CloudEventLogPosition> AppendAsync(Stream block, CancellationToken cancellationToken = default(CancellationToken))
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
            return new CloudEventLogPosition(logNumber, sequenceNumber);
        }

        private async Task<long> AppendInternalAsync(CloudAppendBlob appendBlob, Stream block, CancellationToken cancellationToken)
        {
            if (!(block.Position < block.Length)) throw new ArgumentException("block is empty", nameof(block));

            // this exist to be able to impose arbitrary limits for testing purposes

            var sequenceNumber = await appendBlob.AppendBlockAsync(block, null, cancellationToken);

            if (!(appendBlob.Properties.AppendBlobCommittedBlockCount.Value < Configuration.MaxBlockCount))
            {
                // ignore this block

                // in this case the block has been written but it is not valid
                // this is expected because the append block API doesn't guarantee
                // that the block isn't committed twice for various reasons
                // the table operation is necessary for both transactional integrity
                // and as an indexing service

                throw new StorageException(new RequestResult { HttpStatusCode = 409 }, null, null);
            }

            return sequenceNumber;
        }

        public async Task<List<CloudEventLogPositionLengthData>> FetchAsync(IEnumerable<CloudEventLogPositionLength> source, CancellationToken cancellationToken = default(CancellationToken))
        {
            // figure out the best way to get all data...

            var b = new List<CloudEventLogPositionLength[]>();

            long prevLog = 0;
            long nextPosition = 0;

            var range = new List<CloudEventLogPositionLength>();

            foreach (var item in source)
            {
                if (0 < range.Count)
                {
                    if ((item.Log == prevLog) & (item.Position == nextPosition))
                    {
                        range.Add(item); // consecutive
                    }
                    else
                    {
                        b.Add(range.ToArray());
                        range.Clear();
                        range.Add(item);
                    }
                }
                else
                {
                    range.Add(item);
                }
                prevLog = item.Log;
                nextPosition = item.Position + item.Size;
            }

            if (0 < range.Count)
            {
                b.Add(range.ToArray());
            }

            //

            var container = await _container;

            var buffers = new byte[b.Count][];

            await b.ForEachAsync(async (item, i) =>
            {
                var size = item.Sum(x => x.Size);
                var blob = container.GetAppendBlobReference(GetAppendBlobName(item[0].Log));
                var target = new byte[size];
                await blob.DownloadRangeToByteArrayAsync(target, 0, item[0].Position, size, cancellationToken);
                buffers[i] = target;
            }, 5, cancellationToken);

            //

            var results = new List<CloudEventLogPositionLengthData>();

            {
                int i = 0;
                foreach (var item in b)
                {
                    var buffer = buffers[i];

                    int offset = 0;
                    foreach (var slice in item)
                    {
                        var size = slice.Size;
                        results.Add(new CloudEventLogPositionLengthData(slice.Log, slice.Position, new ArraySegment<byte>(buffer, offset, size)));
                        offset += size;
                    }

                    i++;
                }
            }

            return results;
        }
    }
}

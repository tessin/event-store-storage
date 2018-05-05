using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace CloudEventStore
{
    public interface ICloudEventBlobClient
    {
        Task<CloudEventLogPosition> AppendAsync(
            Stream block,
            CancellationToken cancellationToken = default(CancellationToken)
        );

        Task<List<CloudEventLogPositionLengthData>> FetchAsync(
            IEnumerable<CloudEventLogPositionLength> source,
            CancellationToken cancellationToken = default(CancellationToken)
        );
    }

    public interface ICloudEventTableClient
    {
        Task<bool> CommitAsync(
            CloudEventLogPosition lsn,
            IEnumerable<CloudEventHeader> headers,
            CancellationToken cancellationToken = default(CancellationToken)
        );

        Task<CloudEventLogPositionLengthSegment> GetLogSegmentedAsync(
           CloudEventLogPosition lsn,
           int takeCount,
           CancellationToken cancellationToken = default(CancellationToken)
       );

        Task<CloudEventLogPositionLengthSegment> GetStreamSegmentedAsync(
            CloudEventStreamSequence next,
            int takeCount,
            CancellationToken cancellationToken = default(CancellationToken)
        );
    }

    public interface ICloudEventStoreClient
    {
        Task<List<long>> AppendAsync(
            IEnumerable<UncommittedEvent> uncommitted,
            CancellationToken cancellationToken = default(CancellationToken)
        );
    }
}

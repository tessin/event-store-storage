using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace CloudEventStore
{
    public interface ICloudEventBlobClient
    {
        Task<CloudEventLogSequenceNumber> AppendAsync(Stream block, CancellationToken cancellationToken = default(CancellationToken));
    }

    public interface ICloudEventTableClient
    {
        Task<bool> CommitAsync(CloudEventLogSequenceNumber lsn, IEnumerable<CloudEventHeader> headers, CancellationToken cancellationToken = default(CancellationToken));
    }

    public interface ICloudEventStore
    {
        Task<List<long>> AppendAsync(IEnumerable<UncommittedEvent> uncommitted, CancellationToken cancellationToken = default(CancellationToken));
    }
}

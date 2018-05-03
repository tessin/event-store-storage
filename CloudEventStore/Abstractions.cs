using System.IO;
using System.Threading.Tasks;

namespace CloudEventStore
{
    public interface ICloudEventStoreClient
    {
        Task<CloudEventLogSequenceNumber> AppendAsync(Stream inputStream);
    }
}

using CloudEventStore.Internal;
using Microsoft.WindowsAzure.Storage.Table;
using System;

namespace CloudEventStore
{
    class CloudEventStreamEntity : TableEntity
    {
        public CloudEventStreamEntity()
        {
        }

        public CloudEventStreamEntity(Guid streamId, int sequenceNumber, CloudEventLogSequenceNumber lsn)
        {
            PartitionKey = CloudEventTransaction.PartitionKey;
            RowKey = FormattableString.Invariant($"s-{streamId.ToByteArray().Encode32()}-{sequenceNumber:0000000000}");
            LSN = lsn.Value;
        }

        public long LSN { get; set; }
    }
}

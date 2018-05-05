using CloudEventStore.Internal;
using Microsoft.WindowsAzure.Storage.Table;
using System;

namespace CloudEventStore
{
    class CloudEventTransactionLogEntity : TableEntity
    {
        public CloudEventTransactionLogEntity()
        {
        }

        public CloudEventTransactionLogEntity(CloudEventLogSequenceNumber tranStart, CloudEventLogSequenceNumber tranEnd, byte[] idx)
        {
            var tranEnd2 = new CloudEventLogSequenceNumber(tranEnd.LogNumber, tranEnd.SequenceNumber - 1); // see GetEnd

            PartitionKey = CloudEventTransaction.PartitionKey;
            RowKey = CloudEventTransaction.RowKeyTransactionPrefix + tranEnd.LogNumberFixed8 + "-" + tranEnd2.SequenceNumberFixed12;
            LSN = tranStart.Value;
            IDX = idx;
        }

        public long LSN { get; set; }
        public byte[] IDX { get; set; }

        public CloudEventLogSequenceNumber GetStart()
        {
            return new CloudEventLogSequenceNumber(LSN);
        }

        public CloudEventLogSequenceNumber GetEnd()
        {
            // l-00000000-000000000000
            // 01234567890123456789012
            //           1111111112222

            var log = RowKey.FromFixed(2, 8);
            var sequenceNumber = RowKey.FromFixed(11, 12);

            return new CloudEventLogSequenceNumber(log, sequenceNumber + 1);
        }
    }
}

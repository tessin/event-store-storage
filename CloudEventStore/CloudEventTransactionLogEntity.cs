using CloudEventStore.Internal;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;

namespace CloudEventStore
{
    class CloudEventTransactionLogEntity : ITableEntity
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset Timestamp { get; set; }
        public string ETag { get; set; }

        public int Log { get; set; }
        public long Position { get; set; }
        public byte[] Size { get; set; }

        public CloudEventTransactionLogEntity()
        {
        }

        public CloudEventTransactionLogEntity(CloudEventLogPosition tranStart, CloudEventLogPosition tranEnd, byte[] idx)
        {
            var tranEnd2 = new CloudEventLogPosition(tranEnd.Log, tranEnd.Position - 1); // see GetEnd

            PartitionKey = CloudEventTransaction.PartitionKey;
            RowKey = CloudEventTransaction.RowKeyTransactionPrefix + tranEnd.LogFixed8 + "-" + tranEnd2.PositionFixed12;
            Position = tranStart.Value;
            Size = idx;
        }

        public CloudEventLogPosition GetStart()
        {
            return new CloudEventLogPosition(Position);
        }

        public CloudEventLogPosition GetEnd()
        {
            // l-00000000-000000000000
            // 01234567890123456789012
            //           1111111112222

            var log = RowKey.FromFixed(2, 8);
            var sequenceNumber = RowKey.FromFixed(11, 12);

            return new CloudEventLogPosition(log, sequenceNumber + 1);
        }

        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            Log = properties["L"].Int32Value.Value;
            Position = properties["P"].Int64Value.Value;
            Size = properties["T"].BinaryValue;
        }

        public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var d = new Dictionary<string, EntityProperty>();
            d.Add("L", new EntityProperty(Log));
            d.Add("P", new EntityProperty(Position));
            d.Add("T", new EntityProperty(Size));
            return d;
        }
    }
}

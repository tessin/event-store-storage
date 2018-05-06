using CloudEventStore.Internal;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;

namespace CloudEventStore
{
    class CloudEventStreamEntity : ITableEntity
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset Timestamp { get; set; }
        public string ETag { get; set; }

        public int Log { get; set; }
        public long Position { get; set; }
        public int Length { get; set; }

        public CloudEventStreamEntity()
        {
        }

        public CloudEventStreamEntity(Guid streamId, int sequenceNumber, CloudEventLogPosition lsn, int length)
        {
            PartitionKey = CloudEventTransaction.PartitionKey;
            RowKey = CloudEventTransaction.RowKeyStreamPrefix + streamId.ToByteArray().Encode32() + "-" + ((long)sequenceNumber).ToFixed(10);
            Log = (int)lsn.Log;
            Position = lsn.Position;
            Length = length;
        }

        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            Log = properties["L"].Int32Value.Value;
            Position = properties["P"].Int64Value.Value;
            Length = properties["S"].Int32Value.Value;
        }

        public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var d = new Dictionary<string, EntityProperty>();
            d.Add("L", new EntityProperty(Log));
            d.Add("P", new EntityProperty(Position));
            d.Add("S", new EntityProperty(Length));
            return d;
        }
    }
}

﻿using AppendTransactionLogBlob.Internal;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppendTransactionLogBlob
{
    public class CloudEventStoreContext
    {
        private readonly CloudEventStorage _storage;

        public CloudEventStoreContext(CloudEventStorage storage)
        {
            _storage = storage;
        }

        private TableQuerySegment<CloudEventTransactionLogEntity> _index;

        public async Task<bool> SeekAsync(CloudEventLogSequenceNumber lsn)
        {
            // todo: when seeking, we might not need to fetch more data we might be able to seek within the data we already have

            var index = await _storage._index;

            var q = new TableQuery<CloudEventTransactionLogEntity>
            {
                FilterString =
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, CloudEventTransaction.PartitionKey),
                        TableOperators.And,
                        TableQuery.CombineFilters(
                            TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, CloudEventTransaction.RowKeyTransactionPrefix + CloudEventLogSequenceNumber.MinValue),
                            TableOperators.And,
                            TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, CloudEventTransaction.RowKeyTransactionPrefix + CloudEventLogSequenceNumber.MaxValue)
                        )
                    ),
                TakeCount = 100 // tuning parameter
            };

            var ct = new TableContinuationToken { NextPartitionKey = CloudEventTransaction.PartitionKey, NextRowKey = CloudEventTransaction.RowKeyTransactionPrefix + lsn };

            _index = await index.ExecuteQuerySegmentedAsync(q, ct);

            return 0 < _index.Results.Count;
        }

        public async Task<CloudEventLogReader> OpenReadAsync(CloudEventLogSequenceNumber lsn)
        {
            CloudEventTransactionLogEntity tran = null;

            foreach (var result in _index.Results)
            {
                var resultEnd = result.GetEnd();
                if ((resultEnd.Log == lsn.Log) & (lsn.SequenceNumber < resultEnd.SequenceNumber))
                {
                    tran = result;
                    break;
                }
            }

            if (tran == null)
            {
                throw new InvalidOperationException("undefined");
            }

            var tranStart = tran.GetStart();
            var tranEnd = tran.GetEnd();

            var log = await _storage.GetLogBlobAsync(tranStart);

            var logStream = await log.OpenReadAsync();

            logStream.Position = lsn.SequenceNumber; // todo: this will fail when the lsn is past the end of a full append blob and the index seeks to the next append blob

            var logStreamReader = new CloudEventLogReader(logStream, tranStart.Log, tranEnd.SequenceNumber);
            return logStreamReader;
        }
    }
}

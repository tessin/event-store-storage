using System;
using System.Threading.Tasks;
using AppendTransactionLogBlob.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage.Table;

namespace AppendTransactionLogBlob
{
    [TestClass, TestCategory("CloudTable")]
    public class CloudTableTests : CloudStorageAccountTestBase
    {
        [TestMethod]
        public async Task CloudTable_RangeAndSeekQuery_Test()
        {
            var table = await GetTable();

            var b = new TableBatchOperation();

            b.Insert(new DynamicTableEntity { PartitionKey = "commit", RowKey = new CloudEventLogSequenceNumber(0, 0).ToString() }, false);
            b.Insert(new DynamicTableEntity { PartitionKey = "commit", RowKey = new CloudEventLogSequenceNumber(0, 2).ToString() }, false);
            b.Insert(new DynamicTableEntity { PartitionKey = "commit", RowKey = new CloudEventLogSequenceNumber(0, 3).ToString() }, false);
            b.Insert(new DynamicTableEntity { PartitionKey = "commit", RowKey = new CloudEventLogSequenceNumber(0, 5).ToString() }, false);
            b.Insert(new DynamicTableEntity { PartitionKey = "commit", RowKey = new CloudEventLogSequenceNumber(0, 11).ToString() }, false);

            b.Insert(new DynamicTableEntity { PartitionKey = "commit", RowKey = new CloudEventLogSequenceNumber(1, 0).ToString() }, false);
            b.Insert(new DynamicTableEntity { PartitionKey = "commit", RowKey = new CloudEventLogSequenceNumber(1, 2).ToString() }, false);
            b.Insert(new DynamicTableEntity { PartitionKey = "commit", RowKey = new CloudEventLogSequenceNumber(1, 3).ToString() }, false);
            b.Insert(new DynamicTableEntity { PartitionKey = "commit", RowKey = new CloudEventLogSequenceNumber(1, 5).ToString() }, false);
            b.Insert(new DynamicTableEntity { PartitionKey = "commit", RowKey = new CloudEventLogSequenceNumber(1, 11).ToString() }, false);

            await table.ExecuteBatchAsync(b);

            {
                var q = new TableQuery
                {
                    FilterString =
                        TableQuery.CombineFilters(
                            TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "commit"),
                            TableOperators.And,
                            TableQuery.CombineFilters(
                                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, new CloudEventLogSequenceNumber(0, 2).ToString()),
                                TableOperators.And,
                                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, new CloudEventLogSequenceNumber(0, 5).ToString())
                            )
                        )
                };

                var results = await table.ExecuteQuerySegmentedAsync(q, null);

                Assert.AreEqual(3, results.Results.Count);

                Assert.AreEqual("00000000-000000000002", results.Results[0].RowKey);
                Assert.AreEqual("00000000-000000000003", results.Results[1].RowKey);
                Assert.AreEqual("00000000-000000000005", results.Results[2].RowKey);
            }

            // find next row
            {
                var q = new TableQuery
                {
                    FilterString = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "commit"),
                    TakeCount = 1
                };

                var ct = new TableContinuationToken { NextPartitionKey = "commit", NextRowKey = new CloudEventLogSequenceNumber(0, 0).ToString() };

                var results = await table.ExecuteQuerySegmentedAsync(q, ct);

                Assert.AreEqual(1, results.Results.Count);

                Assert.AreEqual("00000000-000000000000", results.Results[0].RowKey);
            }

            // find next row
            {
                var q = new TableQuery
                {
                    FilterString = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "commit"),
                    TakeCount = 1
                };

                var ct = new TableContinuationToken { NextPartitionKey = "commit", NextRowKey = new CloudEventLogSequenceNumber(1, 9).ToString() };

                var results = await table.ExecuteQuerySegmentedAsync(q, ct);

                Assert.AreEqual(1, results.Results.Count);

                Assert.AreEqual("00000001-000000000011", results.Results[0].RowKey);
            }

            // find next row
            {
                var q = new TableQuery
                {
                    FilterString = TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "commit"),
                        TableOperators.And,
                        TableQuery.CombineFilters(
                            TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, CloudEventLogSequenceNumber.MinValue.ToString()),
                            TableOperators.And,
                            TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, CloudEventLogSequenceNumber.MaxValue.ToString())
                        )
                    ),
                    TakeCount = 1
                };

                var ct = new TableContinuationToken { NextPartitionKey = "commit", NextRowKey = new CloudEventLogSequenceNumber(0, 12).ToString() };

                var results = await table.ExecuteQuerySegmentedAsync(q, ct);

                Assert.AreEqual(1, results.Results.Count);

                Assert.AreEqual("00000001-000000000000", results.Results[0].RowKey);
            }
        }
    }
}

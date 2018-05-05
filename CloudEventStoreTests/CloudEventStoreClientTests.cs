using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace CloudEventStore
{
    [TestClass, TestCategory("V2")]
    public class CloudEventStoreClientTests : CloudStorageAccountTestBase
    {
        [TestMethod]
        public async Task CloudEventBlobClient_AppendAsync_Test()
        {
            var containerName = GetContainerName();

            var client = new CloudEventBlobClient(StorageAccount);
            client.Configuration.ContainerName = containerName;
            await client.AppendAsync(new MemoryStream(new byte[] { 2 }));

            var client2 = new CloudEventBlobClient(StorageAccount);
            client2.Configuration.ContainerName = containerName;
            await client2.AppendAsync(new MemoryStream(new byte[] { 3 }));

            var client3 = new CloudEventBlobClient(StorageAccount);
            client3.Configuration.ContainerName = containerName;
            client3.Configuration.MaxBlockCount = 2;
            await client3.AppendAsync(new MemoryStream(new byte[] { 5 }));
            await client3.AppendAsync(new MemoryStream(new byte[] { 7 }));
            await client3.AppendAsync(new MemoryStream(new byte[] { 11 }));
        }

        [TestMethod]
        public async Task CloudEventTableClient_CommitAsync_Test()
        {
            var tableName = GetTableName();

            var streamId = Guid.NewGuid();

            bool tran1, tran2;

            var client = new CloudEventTableClient(StorageAccount);
            client.Configuration.TableName = tableName;
            tran1 = await client.CommitAsync(
                new CloudEventLogSequenceNumber(0, 0),
                new[] {
                    new CloudEventHeader(streamId, 1, 0, 2),
                    new CloudEventHeader(streamId, 2, 0, 3)
                }
            );

            var client2 = new CloudEventTableClient(StorageAccount);
            client2.Configuration.TableName = tableName;
            tran2 = await client2.CommitAsync(
                new CloudEventLogSequenceNumber(0, 1),
                new[] {
                    new CloudEventHeader(streamId, 1, 0, 2),
                    new CloudEventHeader(streamId, 2, 0, 3)
                }
            );

            Assert.IsTrue(tran1);
            Assert.IsFalse(tran2);
        }
    }
}

using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Diagnostics;
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
        public async Task CloudEventBlobClient_GetLogSegmentedAsync_Test()
        {
            var containerName = GetContainerName();

            var client = new CloudEventBlobClient(StorageAccount);
            client.Configuration.ContainerName = containerName;
            await client.AppendAsync(new MemoryStream(new byte[] { 2, 3, 5, 7, 11 }));

            var result = await client.GetLogSegmentedAsync(new[] {
                new CloudEventLogPositionLength(0, 0, 1),
                new CloudEventLogPositionLength(0, 1, 1),
                new CloudEventLogPositionLength(0, 2, 1),
                new CloudEventLogPositionLength(0, 3, 1),
                new CloudEventLogPositionLength(0, 4, 1),
            });

            Assert.AreEqual(2, result[0].Data.GetItem(0));
            Assert.AreEqual(3, result[1].Data.GetItem(0));
            Assert.AreEqual(5, result[2].Data.GetItem(0));
            Assert.AreEqual(7, result[3].Data.GetItem(0));
            Assert.AreEqual(11, result[4].Data.GetItem(0));
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
                new CloudEventLogPosition(0, 0),
                new[] {
                    new CloudEventHeader(streamId, 1, 0, 2),
                    new CloudEventHeader(streamId, 2, 0, 3)
                }
            );

            var client2 = new CloudEventTableClient(StorageAccount);
            client2.Configuration.TableName = tableName;
            tran2 = await client2.CommitAsync(
                new CloudEventLogPosition(0, 1),
                new[] {
                    new CloudEventHeader(streamId, 1, 0, 2),
                    new CloudEventHeader(streamId, 2, 0, 3)
                }
            );

            Assert.IsTrue(tran1);
            Assert.IsFalse(tran2);
        }

        [TestMethod]
        public async Task CloudEventTableClient_GetLogSegmentedAsync_Test()
        {
            var tableName = GetTableName();

            var streamId = Guid.NewGuid();

            var client = new CloudEventTableClient(StorageAccount);
            client.Configuration.TableName = tableName;
            await client.CommitAsync(
                new CloudEventLogPosition(0, 0),
                new[] {
                    new CloudEventHeader(streamId, 1,  0, 2),
                    new CloudEventHeader(streamId, 2,  2, 3),
                    new CloudEventHeader(streamId, 3,  5, 5),
                    new CloudEventHeader(streamId, 4, 10, 7),
                }
            );

            var segment = await client.GetLogSegmentedAsync(new CloudEventLogPosition());

            Assert.AreEqual(0, segment.Results[0].Log);
            Assert.AreEqual(0, segment.Results[1].Log);
            Assert.AreEqual(0, segment.Results[2].Log);
            Assert.AreEqual(0, segment.Results[3].Log);

            Assert.AreEqual(0, segment.Results[0].Position);
            Assert.AreEqual(2, segment.Results[1].Position);
            Assert.AreEqual(5, segment.Results[2].Position);
            Assert.AreEqual(10, segment.Results[3].Position);
        }

        private CloudEventStoreClient GetEventStoreClient()
        {
            var blobClient = new CloudEventBlobClient(StorageAccount) { Configuration = { ContainerName = GetContainerName() } };
            var tableClient = new CloudEventTableClient(StorageAccount) { Configuration = { TableName = GetTableName() } };

            return new CloudEventStoreClient(blobClient, tableClient);
        }

        [TestMethod]
        public async Task CloudEventStoreClient_AppendAsync_Test()
        {
            var client = GetEventStoreClient();

            var streamId = Guid.NewGuid();

            var committed = await client.AppendAsync(new[] {
                new UncommittedEvent { StreamId = streamId, SequenceNumber = 1, Created = DateTimeOffset.UtcNow }
            });

            CollectionAssert.AreEqual(new[] { 0L }, committed);
        }

        [TestMethod]
        public async Task CloudEventStoreClient_GetLogSegmentedAsync_Test()
        {
            var client = GetEventStoreClient();

            var streamId = Guid.NewGuid();

            await client.AppendAsync(new[] {
                new UncommittedEvent { StreamId = streamId, SequenceNumber = 1, Created = DateTimeOffset.UtcNow },
                //new UncommittedEvent { StreamId = streamId, SequenceNumber = 2, Created = DateTimeOffset.UtcNow },
                //new UncommittedEvent { StreamId = streamId, SequenceNumber = 3, Created = DateTimeOffset.UtcNow },
            });

            var segment = await client.GetLogSegmentedAsync(new CloudEventLogPosition());

            Assert.AreEqual(1, segment.Results.Count);

            Assert.AreEqual(streamId, segment.Results[0].StreamId);
            Assert.AreEqual(1, segment.Results[0].SequenceNumber);

            await client.AppendAsync(new[] {
                new UncommittedEvent { StreamId = streamId, SequenceNumber = 2, Created = DateTimeOffset.UtcNow },
                new UncommittedEvent { StreamId = streamId, SequenceNumber = 3, Created = DateTimeOffset.UtcNow },
            });

            var segment2 = await client.GetLogSegmentedAsync(segment.Next);

            Assert.AreEqual(2, segment2.Results.Count);

            Assert.AreEqual(streamId, segment2.Results[0].StreamId);
            Assert.AreEqual(2, segment2.Results[0].SequenceNumber);
            Assert.AreEqual(streamId, segment2.Results[1].StreamId);
            Assert.AreEqual(3, segment2.Results[1].SequenceNumber);

            var segment3 = await client.GetLogSegmentedAsync(segment2.Next);

            Assert.AreEqual(0, segment3.Results.Count);
        }


        [TestMethod]
        public async Task CloudEventStoreClient_GetStreamSegmentedAsync_Test()
        {
            var client = GetEventStoreClient();

            var streamId = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            await client.AppendAsync(new[] {
                new UncommittedEvent { StreamId = streamId[0], SequenceNumber = 1, Created = DateTimeOffset.UtcNow },
                new UncommittedEvent { StreamId = streamId[1], SequenceNumber = 1, Created = DateTimeOffset.UtcNow },
                new UncommittedEvent { StreamId = streamId[2], SequenceNumber = 1, Created = DateTimeOffset.UtcNow },
                new UncommittedEvent { StreamId = streamId[0], SequenceNumber = 2, Created = DateTimeOffset.UtcNow },
                new UncommittedEvent { StreamId = streamId[1], SequenceNumber = 2, Created = DateTimeOffset.UtcNow },
                new UncommittedEvent { StreamId = streamId[2], SequenceNumber = 2, Created = DateTimeOffset.UtcNow },
                new UncommittedEvent { StreamId = streamId[0], SequenceNumber = 3, Created = DateTimeOffset.UtcNow },
                new UncommittedEvent { StreamId = streamId[1], SequenceNumber = 3, Created = DateTimeOffset.UtcNow },
                new UncommittedEvent { StreamId = streamId[2], SequenceNumber = 3, Created = DateTimeOffset.UtcNow },
            });

            {
                var stream = await client.GetStreamSegmentedAsync(new CloudEventStreamSequence(streamId[0], 1));

                Assert.AreEqual(streamId[0], stream.Results[0].StreamId);
                Assert.AreEqual(1, stream.Results[0].SequenceNumber);

                Assert.AreEqual(streamId[0], stream.Results[1].StreamId);
                Assert.AreEqual(2, stream.Results[1].SequenceNumber);

                Assert.AreEqual(streamId[0], stream.Results[2].StreamId);
                Assert.AreEqual(3, stream.Results[2].SequenceNumber);
            }
        }
    }
}

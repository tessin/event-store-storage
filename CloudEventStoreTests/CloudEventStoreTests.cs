using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CloudEventStore;
using CloudEventStore.Internal;
using CloudEventStore.Metrics;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudEventStore
{
    [TestClass, TestCategory("CloudEventStorage")]
    public class CloudEventStoreTests : CloudStorageAccountTestBase
    {
        [TestMethod]
        public async Task CloudEventStore_Append_Test()
        {
            var r = new Random();

            var storage = await GetEventStorage();

            var store = new CloudEventStore(storage);

            var streamId = Guid.NewGuid();
            var typeId1 = Guid.NewGuid();
            var typeId2 = Guid.NewGuid();

            await store.AppendAsync(
                new[] {
                    new UncomittedEvent { StreamId = streamId, SequenceNumber = 1, TypeId = typeId1, Payload = new ArraySegment<byte>(CryptoUtils.GetBytes(r.Next(256))), Created = DateTimeOffset.UtcNow },
                    new UncomittedEvent { StreamId = streamId, SequenceNumber = 2, TypeId = typeId2, Payload = new ArraySegment<byte>(CryptoUtils.GetBytes(r.Next(256))), Created = DateTimeOffset.UtcNow },
                }
            );

            // assert blob stuff
        }

        class TestData1
        {
            public Guid StreamId { get; set; }
            public Guid TypeId1 { get; set; }
            public Guid TypeId2 { get; set; }
        }

        private async Task<TestData1> TestData1Async(CloudEventStore store)
        {
            var r = new Random();

            var streamId = Guid.NewGuid();
            var typeId1 = Guid.NewGuid();
            var typeId2 = Guid.NewGuid();

            await store.AppendAsync(
                new[] {
                    new UncomittedEvent { StreamId = streamId, SequenceNumber = 1, TypeId = typeId1, Payload = new ArraySegment<byte>(CryptoUtils.GetBytes(r.Next(256))), Created = DateTimeOffset.UtcNow },
                    new UncomittedEvent { StreamId = streamId, SequenceNumber = 2, TypeId = typeId2, Payload = new ArraySegment<byte>(CryptoUtils.GetBytes(r.Next(256))), Created = DateTimeOffset.UtcNow },
                }
            );

            await store.AppendAsync(
                new[] {
                    new UncomittedEvent { StreamId = streamId, SequenceNumber = 3, TypeId = typeId1, Payload = new ArraySegment<byte>(CryptoUtils.GetBytes(r.Next(256))), Created = DateTimeOffset.UtcNow },
                    new UncomittedEvent { StreamId = streamId, SequenceNumber = 4, TypeId = typeId2, Payload = new ArraySegment<byte>(CryptoUtils.GetBytes(r.Next(256))), Created = DateTimeOffset.UtcNow },
                    new UncomittedEvent { StreamId = streamId, SequenceNumber = 5, TypeId = typeId2, Payload = new ArraySegment<byte>(CryptoUtils.GetBytes(r.Next(256))), Created = DateTimeOffset.UtcNow },
                }
            );

            await store.AppendAsync(
                new[] {
                    new UncomittedEvent { StreamId = streamId, SequenceNumber = 6, TypeId = typeId1, Payload = new ArraySegment<byte>(CryptoUtils.GetBytes(r.Next(256))), Created = DateTimeOffset.UtcNow },
                    new UncomittedEvent { StreamId = streamId, SequenceNumber = 7, TypeId = typeId2, Payload = new ArraySegment<byte>(CryptoUtils.GetBytes(r.Next(256))), Created = DateTimeOffset.UtcNow },
                    new UncomittedEvent { StreamId = streamId, SequenceNumber = 8, TypeId = typeId2, Payload = new ArraySegment<byte>(CryptoUtils.GetBytes(r.Next(256))), Created = DateTimeOffset.UtcNow },
                    new UncomittedEvent { StreamId = streamId, SequenceNumber = 9, TypeId = typeId2, Payload = new ArraySegment<byte>(CryptoUtils.GetBytes(r.Next(256))), Created = DateTimeOffset.UtcNow },
                    new UncomittedEvent { StreamId = streamId, SequenceNumber = 10, TypeId = typeId2, Payload = new ArraySegment<byte>(CryptoUtils.GetBytes(r.Next(256))), Created = DateTimeOffset.UtcNow },
                }
            );

            return new TestData1
            {
                StreamId = streamId,
                TypeId1 = typeId1,
                TypeId2 = typeId1,
            };
        }

        [TestMethod]
        public async Task CloudEventStore_Continuation_Test()
        {
            var storage = await GetEventStorage();
            var store = new CloudEventStore(storage);

            var test = await TestData1Async(store);

            var result = await store.GetEventLogSegmentedAsync(0, maxCount: 1);

            Assert.AreEqual(1, result.Results.Count);
            Assert.AreEqual(test.StreamId, result.Results[0].StreamId);
            Assert.AreEqual(1, result.Results[0].SequenceNumber);

            var result2 = await store.GetEventLogSegmentedAsync(result.ContinuationToken, maxCount: 1);

            Assert.AreEqual(1, result2.Results.Count);
            Assert.AreEqual(test.StreamId, result2.Results[0].StreamId);
            Assert.AreEqual(2, result2.Results[0].SequenceNumber);

            var result3 = await store.GetEventLogSegmentedAsync(result2.ContinuationToken, maxCount: 1);

            Assert.AreEqual(1, result3.Results.Count);
            Assert.AreEqual(test.StreamId, result3.Results[0].StreamId);
            Assert.AreEqual(3, result3.Results[0].SequenceNumber);
        }

        [TestMethod]
        public async Task CloudEventStore_Range_Test()
        {
            var storage = await GetEventStorage();
            var store = new CloudEventStore(storage);

            var test = await TestData1Async(store);

            var result = await store.GetEventLogSegmentedAsync(0);

            Assert.AreEqual(10, result.Results.Count);
        }

        static IEnumerable<UncomittedEvent> GetTestDataStream(int n)
        {
            var typeId = Guid.NewGuid();
            for (int i = 0; i < n; i++)
            {
                var streamId = Guid.NewGuid();
                yield return new UncomittedEvent { StreamId = streamId, SequenceNumber = 1, TypeId = typeId, Payload = new ArraySegment<byte>(), Created = DateTimeOffset.UtcNow };
            }
        }

        [TestMethod]
        [Ignore("Benchmark")]
        public async Task CloudEventStore_Spill_Test()
        {
            Configuration.Dump = false;

            //Best case
            //AppendBlock: N=3059, Average=55.676, Stdev=62.817 [0.963, 0.026, 0.004]
            //ExecuteBatch: N=3059, Average=44.515, Stdev=16.826 [0.909, 0.061, 0.020]
            //~500 req/sec

            // note that this is inserting a single event with each transaction

            var storage = await GetEventStorage();
            var stats = new BucketManager(TimeSpan.FromSeconds(3));
            var store = new CloudEventStore(storage, stats);

            await GetTestDataStream(100003).ForEachAsync((e) => store.AppendAsync(new[] { e }), 250);

            foreach (var bucket in stats.GetBuckets())
            {
                Trace.WriteLine(bucket.AppendBlock, "AppendBlock");
            }

            foreach (var bucket in stats.GetBuckets())
            {
                Trace.WriteLine(bucket.ExecuteBatch, "ExecuteBatch");
            }
        }

        [TestMethod]
        [Ignore("Benchmark")]
        public async Task CloudEventStore_Flood_Test()
        {
            Configuration.Dump = false;

            var storage = await GetEventStorage();
            var stats = new BucketManager(TimeSpan.FromSeconds(3));
            var store = new CloudEventStore(storage, stats);

            //Best case
            //AppendBlock: N=255, Average=41.649, Stdev=102.883 [0.769, 0.180, 0.024]
            //ExecuteBatch: N=255, Average=162.098, Stdev=35.052 [0.745, 0.220, 0.027]
            //~5000 req/sec

            // note that this is inserting a 99 events with each transaction

            await Enumerable.Range(0, 1100).ForEachAsync((e) => store.AppendAsync(GetTestDataStream(99)), 25);

            foreach (var bucket in stats.GetBuckets())
            {
                Trace.WriteLine(bucket.AppendBlock, "AppendBlock");
            }

            foreach (var bucket in stats.GetBuckets())
            {
                Trace.WriteLine(bucket.ExecuteBatch, "ExecuteBatch");
            }
        }
    }
}

using System;
using CloudEventStore.Metrics;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudEventStore
{
    [TestClass]
    public class BucketManagerTests
    {
        [TestMethod]
        public void BucketManagerTest()
        {
            var bucketmgr = new BucketManager(TimeSpan.FromSeconds(3));

            var b1 = bucketmgr.GetBucket(DateTime.UtcNow);
            b1.AppendBlock.Push(1);

            var b2 = bucketmgr.GetBucket(DateTime.UtcNow.AddSeconds(3));
            b2.AppendBlock.Push(2);

            var buckets = bucketmgr.GetBuckets(DateTime.UtcNow.AddSeconds(6));

            Assert.AreEqual(2, buckets.Count);
        }
    }
}

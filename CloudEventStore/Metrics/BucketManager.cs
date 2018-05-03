using System;
using System.Collections.Generic;
using System.Threading;

namespace CloudEventStore.Metrics
{
    public class BucketManager
    {
        private readonly TimeSpan _window;
        private Bucket _bucket;

        public BucketManager(TimeSpan window)
        {
            _window = window;
        }

        public Bucket GetBucket(DateTime t)
        {
            var k = t.Ticks / _window.Ticks;

            int n = 0;

            _GetBucket:

            var bucket = _bucket;
            if (bucket == null)
            {
                var bucket2 = new Bucket(k);
                if (Interlocked.CompareExchange(ref _bucket, bucket2, null) != null)
                {
                    if (n < 1)
                    {
                        n++;
                        goto _GetBucket;
                    }
                    return bucket2; // discard
                }
                return bucket2;
            }

            if (bucket.Key < k)
            {
                var bucket3 = new Bucket(k, bucket);
                if (Interlocked.CompareExchange(ref _bucket, bucket3, bucket) != bucket)
                {
                    if (n < 1)
                    {
                        n++;
                        goto _GetBucket;
                    }
                    return bucket3; // discard
                }
                return bucket3;
            }

            return bucket;
        }

        public List<Bucket> GetBuckets()
        {
            return GetBuckets(DateTime.UtcNow);
        }

        public List<Bucket> GetBuckets(DateTime t)
        {
            var k = t.Ticks / _window.Ticks;

            var bucket = _bucket;
            if (bucket == null)
            {
                return new List<Bucket>();
            }

            var list = new List<Bucket>();

            while (bucket != null)
            {
                if (bucket.Key < k)
                {
                    list.Add(bucket);
                }
                bucket = bucket.Next;
            }

            list.Reverse();

            return list;
        }

        public void SubmitAppend(DateTime t, TimeSpan appendBlockElapsed, TimeSpan executeBatchElapsed)
        {
            var bucket = GetBucket(t);
            lock (bucket)
            {
                bucket.AppendBlock.Push(appendBlockElapsed.TotalMilliseconds);
                bucket.ExecuteBatch.Push(executeBatchElapsed.TotalMilliseconds);
            }
        }
    }
}

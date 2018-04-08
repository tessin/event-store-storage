using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tessin.Statistics;

namespace AppendTransactionLogBlob.Metrics
{
    public class Bucket
    {
        public readonly long Key;
        public readonly Bucket Next;

        public Bucket(long k, Bucket n = null)
        {
            Key = k;
            Next = n;
        }

        public readonly RunningStat AppendBlock = new RunningStat();
        public readonly RunningStat ExecuteBatch = new RunningStat();
    }
}

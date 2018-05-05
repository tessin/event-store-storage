using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudEventStore
{
    class CloudEventTransaction
    {
        public static readonly string PartitionKey = "c";

        public static readonly string RowKeyTransactionPrefix = "t-";
        public static readonly string RowKeyStreamPrefix = "s-";
    }
}

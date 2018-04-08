using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppendTransactionLogBlob
{
    class CloudEventTransaction
    {
        public static readonly string PartitionKey = "c";

        public static readonly string RowKeyTransactionPrefix = "t-";
    }
}

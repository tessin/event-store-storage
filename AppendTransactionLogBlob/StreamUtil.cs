using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppendTransactionLogBlob
{
    static class StreamUtil
    {
        public static Stream Concat(this Stream first, Stream second)
        {
            return new ConcatStream(first, second);
        }
    }
}

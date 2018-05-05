using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudEventStore
{
    public struct CloudEventLogPositionLength
    {
        public readonly int Log;
        public readonly long Position;
        public readonly int Size;

        public CloudEventLogPositionLength(int log, long position, int size)
        {
            Log = log;
            Position = position;
            Size = size;
        }
    }
}

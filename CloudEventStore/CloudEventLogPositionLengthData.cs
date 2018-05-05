using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudEventStore
{
    public struct CloudEventLogPositionLengthData
    {
        public readonly int Log;
        public readonly long Position;
        public readonly ArraySegment<byte> Data;

        public CloudEventLogPositionLengthData(int log, long position, ArraySegment<byte> data)
        {
            Log = log;
            Position = position;
            Data = data;
        }
    }
}

using CloudEventStore.Internal;
using System;

namespace CloudEventStore
{
    public struct CloudEventLogPosition : IComparable<CloudEventLogPosition>
    {
        //          4*1024*1024 = 209715200000
        //   log2(209715200000) = 37.6
        //                63-38 = 25
        //                 2^25 = 33554432
        //       log2(33554432) = 7.5
        //       33554432*50000 = 1677721600000
        // log10(1677721600000) = 12.2

        public static readonly CloudEventLogPosition MaxValue = new CloudEventLogPosition(long.MaxValue);
        public static readonly CloudEventLogPosition MinValue = new CloudEventLogPosition(0);

        private const long LogNumberMask = ((1L << 25) - 1);
        private const long PositionMask = ((1L << 38) - 1);

        public long Log => (Value >> 38) & LogNumberMask;
        public string LogFixed8 => Log.ToFixed(8);

        public long Position => Value & PositionMask;
        public string PositionFixed12 => Position.ToFixed(12);

        public readonly long Value;

        public CloudEventLogPosition(long value)
        {
            if (!(0 <= value)) throw new ArgumentOutOfRangeException(nameof(value));

            Value = value;
        }

        public CloudEventLogPosition(long log, long position)
        {
            if (!((0 <= log) & (log <= LogNumberMask))) throw new ArgumentOutOfRangeException(nameof(log));
            if (!((0 <= position) & (position <= PositionMask))) throw new ArgumentOutOfRangeException(nameof(position));

            Value = ((log & LogNumberMask) << 38) | (position & PositionMask);
        }

        [Obsolete]
        public CloudEventLogPosition SetSequenceNumber(long sequenceNumber)
        {
            return new CloudEventLogPosition(Log, sequenceNumber);
        }

        public int CompareTo(CloudEventLogPosition other)
        {
            return Value.CompareTo(other.Value);
        }

        public override string ToString()
        {
            return LogFixed8 + "-" + PositionFixed12;
        }
    }
}

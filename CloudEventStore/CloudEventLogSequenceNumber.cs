using CloudEventStore.Internal;
using System;

namespace CloudEventStore
{
    public struct CloudEventLogSequenceNumber : IComparable<CloudEventLogSequenceNumber>
    {
        //          4*1024*1024 = 209715200000
        //   log2(209715200000) = 37.6
        //                63-38 = 25
        //                 2^25 = 33554432
        //       log2(33554432) = 7.5
        //       33554432*50000 = 1677721600000
        // log10(1677721600000) = 12.2

        public static readonly CloudEventLogSequenceNumber MaxValue = new CloudEventLogSequenceNumber(long.MaxValue);
        public static readonly CloudEventLogSequenceNumber MinValue = new CloudEventLogSequenceNumber(0);

        private const long LogNumberMask = ((1L << 25) - 1);
        private const long SequenceNumberMask = ((1L << 38) - 1);

        public long LogNumber => (Value >> 38) & LogNumberMask;
        public string LogNumberFixed8 => LogNumber.ToFixed(8);

        public long SequenceNumber => Value & SequenceNumberMask;
        public string SequenceNumberFixed12 => SequenceNumber.ToFixed(12);

        public readonly long Value;

        public CloudEventLogSequenceNumber(long value)
        {
            if (!(0 <= value)) throw new ArgumentOutOfRangeException(nameof(value));

            Value = value;
        }

        public CloudEventLogSequenceNumber(long logNumber, long sequenceNumber)
        {
            if (!((0 <= logNumber) & (logNumber <= LogNumberMask))) throw new ArgumentOutOfRangeException(nameof(logNumber));
            if (!((0 <= sequenceNumber) & (sequenceNumber <= SequenceNumberMask))) throw new ArgumentOutOfRangeException(nameof(sequenceNumber));

            Value = ((logNumber & LogNumberMask) << 38) | (sequenceNumber & SequenceNumberMask);
        }

        [Obsolete]
        public CloudEventLogSequenceNumber SetSequenceNumber(long sequenceNumber)
        {
            return new CloudEventLogSequenceNumber(LogNumber, sequenceNumber);
        }

        public int CompareTo(CloudEventLogSequenceNumber other)
        {
            return Value.CompareTo(other.Value);
        }

        public override string ToString()
        {
            return LogNumberFixed8 + "-" + SequenceNumberFixed12;
        }
    }
}

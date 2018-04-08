using AppendTransactionLogBlob.Internal;
using System;

namespace AppendTransactionLogBlob
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

        private const long LogMask = ((1L << 25) - 1);
        private const long SequenceNumberMask = ((1L << 38) - 1);

        public long Log => (Value >> 38) & LogMask;
        public string LogFixed8 => Log.ToFixed(8);

        public long SequenceNumber => Value & SequenceNumberMask;
        public string SequenceNumberFixed12 => SequenceNumber.ToFixed(12);

        public readonly long Value;

        public CloudEventLogSequenceNumber(long value)
        {
            if (!(0 <= value)) throw new ArgumentOutOfRangeException(nameof(value));

            Value = value;
        }

        public CloudEventLogSequenceNumber(long log, long sequenceNumber)
        {
            if (!((0 <= log) & (log <= LogMask))) throw new ArgumentOutOfRangeException(nameof(log));
            if (!((0 <= sequenceNumber) & (sequenceNumber <= SequenceNumberMask))) throw new ArgumentOutOfRangeException(nameof(sequenceNumber));

            Value = ((log & LogMask) << 38) | (sequenceNumber & SequenceNumberMask);
        }

        public CloudEventLogSequenceNumber SetSequenceNumber(long sequenceNumber)
        {
            return new CloudEventLogSequenceNumber(Log, sequenceNumber);
        }

        public int CompareTo(CloudEventLogSequenceNumber other)
        {
            return Value.CompareTo(other.Value);
        }

        public override string ToString()
        {
            return LogFixed8 + "-" + SequenceNumberFixed12;
        }
    }
}

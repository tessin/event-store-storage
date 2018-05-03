using CloudEventStore.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudEventStore
{
    public static class CloudEventBlobName
    {
        public static readonly string LogPrefix = "log-";

        public static string Log(CloudEventLogSequenceNumber lsn)
        {
            // descending sort order
            var lsn2 = new CloudEventLogSequenceNumber(CloudEventLogSequenceNumber.MaxValue.LogNumber - lsn.LogNumber, 0);
            return LogPrefix + lsn2.LogNumberFixed8 + ".dat";
        }

        public static long LogFromName(string name)
        {
            return CloudEventLogSequenceNumber.MaxValue.LogNumber - name.FromFixed(LogPrefix.Length, 8);
        }

        public static string Committed(CloudEventLogSequenceNumber lsn)
        {
            return "committed-" + lsn.LogNumberFixed8 + ".dat";
        }

        public static string CommittedIndex(CloudEventLogSequenceNumber lsn)
        {
            return "committed-" + lsn.LogNumberFixed8 + ".idx";
        }
    }
}

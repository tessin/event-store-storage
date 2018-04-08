using AppendTransactionLogBlob.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppendTransactionLogBlob
{
    public static class CloudEventBlobName
    {
        public static readonly string LogPrefix = "log-";

        public static string Log(CloudEventLogSequenceNumber lsn)
        {
            // descending sort order
            var lsn2 = new CloudEventLogSequenceNumber(CloudEventLogSequenceNumber.MaxValue.Log - lsn.Log, 0);
            return LogPrefix + lsn2.LogFixed8 + ".dat";
        }

        public static long LogFromName(string name)
        {
            return CloudEventLogSequenceNumber.MaxValue.Log - name.FromFixed(LogPrefix.Length, 8);
        }

        public static string Committed(CloudEventLogSequenceNumber lsn)
        {
            return "committed-" + lsn.LogFixed8 + ".dat";
        }

        public static string CommittedIndex(CloudEventLogSequenceNumber lsn)
        {
            return "committed-" + lsn.LogFixed8 + ".idx";
        }
    }
}

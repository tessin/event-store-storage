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

        public static string Log(CloudEventLogPosition lsn)
        {
            // descending sort order
            var lsn2 = new CloudEventLogPosition(CloudEventLogPosition.MaxValue.Log - lsn.Log, 0);
            return LogPrefix + lsn2.LogFixed8 + ".dat";
        }

        public static long LogFromName(string name)
        {
            return CloudEventLogPosition.MaxValue.Log - name.FromFixed(LogPrefix.Length, 8);
        }

        public static string Committed(CloudEventLogPosition lsn)
        {
            return "committed-" + lsn.LogFixed8 + ".dat";
        }

        public static string CommittedIndex(CloudEventLogPosition lsn)
        {
            return "committed-" + lsn.LogFixed8 + ".idx";
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace CloudEventStore
{
    public struct CloudEventLogPositionLengthSegment
    {
        public List<CloudEventLogPositionLength> Results { get; }
        public TableContinuationToken ContinuationToken { get; }

        public CloudEventLogPositionLengthSegment(List<CloudEventLogPositionLength> results, TableContinuationToken continuationToken)
        {
            this.Results = results;
            this.ContinuationToken = continuationToken;
        }
    }
}

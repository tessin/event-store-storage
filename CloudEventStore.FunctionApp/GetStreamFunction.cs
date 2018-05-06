using System;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;

namespace CloudEventStore
{
    public static class GetStreamFunction
    {
        [FunctionName("GetStreamFunction")]
        public static async Task<HttpResponseMessage> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "event-store/{collection}/stream/{streamId}")]
            HttpRequestMessage req,
            string collection,
            string streamId,
            TraceWriter log,
            CancellationToken cancellationToken
        )
        {
            var streamId2 = new Guid(streamId); // todo: bad request

            var query = req.GetQueryNameValuePairs();

            var offset = query.GetValueOrDefault("offset", 1);
            var take = query.GetValueOrDefault("take", 1000);

            var client = Shared.Collection(collection);

            var segment = await client.GetStreamSegmentedAsync(new CloudEventStreamSequence(streamId2, offset), take, cancellationToken);

            var res = new GetLogFunctionResponse();

            res.Results = segment.Results;

            res.Next = new Uri(
                req.RequestUri,
                "/api/event-store/" + Uri.EscapeDataString(collection) + "/stream/" + streamId + "?offset=" + Convert.ToString(segment.Next.SequenceNumber, CultureInfo.InvariantCulture) + "&take=" + Convert.ToString(take, CultureInfo.InvariantCulture)
            );

            return req.CreateResponse(HttpStatusCode.OK, res, "application/json");
        }
    }
}

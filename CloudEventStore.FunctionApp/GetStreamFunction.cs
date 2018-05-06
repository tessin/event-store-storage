using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace CloudEventStore
{
    public class GetStreamFunctionResponse
    {
        [JsonProperty("results")]
        public List<CloudEvent> Results { get; set; }

        [JsonProperty("next")]
        public Uri Next { get; set; }
    }

    public static class GetStreamFunction
    {
        [FunctionName("GetStreamFunction")]
        public static async Task<HttpResponseMessage> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "event-store/{collection}/stream/{stream}")]
            HttpRequestMessage req,
            string collection,
            string stream,
            TraceWriter log,
            CancellationToken cancellationToken
        )
        {
            Guid streamId;
            if (!Guid.TryParseExact(stream, "D", out streamId))
            {
                return req.CreateResponse(HttpStatusCode.BadRequest);
            }

            var query = req.GetQueryNameValuePairs();

            var offset = query.GetValueOrDefault("offset", 1);
            var take = query.GetValueOrDefault("take", 1000);

            var client = Shared.Collection(collection);

            var segment = await client.GetStreamSegmentedAsync(new CloudEventStreamSequence(streamId, offset), take, cancellationToken);

            var res = new GetStreamFunctionResponse();

            res.Results = segment.Results;

            res.Next = new Uri(
                req.RequestUri,
                "/api/event-store/" + Uri.EscapeDataString(collection) + "/stream/" + streamId + "?offset=" + Convert.ToString(segment.Next.SequenceNumber, CultureInfo.InvariantCulture) + "&take=" + Convert.ToString(take, CultureInfo.InvariantCulture)
            );

            return req.CreateResponse(HttpStatusCode.OK, res, "application/json");
        }
    }
}

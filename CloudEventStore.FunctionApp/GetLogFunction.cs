using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Newtonsoft.Json;

namespace CloudEventStore
{
    public class GetLogFunctionResponse
    {
        [JsonProperty("results")]
        public List<CloudEvent> Results { get; set; }

        [JsonProperty("next")]
        public Uri Next { get; set; }
    }

    public static class GetLogFunction
    {
        [FunctionName("GetLogFunction")]
        public static async Task<HttpResponseMessage> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "event-store/{collection}/log")]
            HttpRequestMessage req,
            string collection,
            TraceWriter log,
            CancellationToken cancellationToken
        )
        {
            var query = req.GetQueryNameValuePairs();

            var offset = query.GetValueOrDefault("offset", 0L);
            var take = query.GetValueOrDefault("take", 1000);

            var client = Shared.Collection(collection);

            var segment = await client.GetLogSegmentedAsync(new CloudEventLogPosition(offset), take, cancellationToken);

            var res = new GetLogFunctionResponse();

            res.Results = segment.Results;

            res.Next = new Uri(
                req.RequestUri,
                "/api/event-store/" + Uri.EscapeDataString(collection) + "/log" + "?offset=" + Convert.ToString(segment.Next.Value, CultureInfo.InvariantCulture) + "&take=" + Convert.ToString(take, CultureInfo.InvariantCulture)
            );

            return req.CreateResponse(HttpStatusCode.OK, res, "application/json");
        }
    }
}

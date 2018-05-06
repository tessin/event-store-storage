using System.Collections.Generic;
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
    public static class AppendFunction
    {
        [FunctionName("AppendFunction")]
        public static async Task<HttpResponseMessage> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "event-store/{collection}/append")]
            HttpRequestMessage req,
            string collection,
            TraceWriter log,
            CancellationToken cancellationToken
        )
        {
            var uncommitted = await req.Content.ReadAsAsync<List<UncommittedEvent>>();

            var client = Shared.Collection(collection);

            var committed = await client.AppendAsync(uncommitted, cancellationToken);

            if (0 < committed.Count)
            {
                return req.CreateResponse(HttpStatusCode.NoContent);
            }

            return req.CreateResponse(HttpStatusCode.Conflict);
        }
    }
}

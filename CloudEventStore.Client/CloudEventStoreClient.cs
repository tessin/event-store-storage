using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CloudEventStore
{
    public class CloudEventStoreClientException : Exception
    {
        public HttpStatusCode StatusCode { get; }
        public string ReasonPhrase { get; }

        public CloudEventStoreClientException(HttpStatusCode statusCode, string reasonPhrase)
        {
            this.StatusCode = statusCode;
            this.ReasonPhrase = reasonPhrase;
        }
    }

    public class CloudEventStoreClient
    {
        private readonly HttpClient _http;

        public Uri BaseAddress { get; set; } = new Uri("http://localhost:7071");

        public string Collection { get; set; } = "default";

        // todo: host key and whatnot for authentication...

        public CloudEventStoreClient(HttpClient http)
        {
            _http = http;
        }

        private Task<HttpResponseMessage> SendAsync(HttpMethod method, string relativeUrl, object payload, CancellationToken cancellationToken)
        {
            return SendAsync(method, new Uri(BaseAddress, relativeUrl), payload, cancellationToken);
        }

        private Task<HttpResponseMessage> SendAsync(HttpMethod method, Uri absoluteUrl, object payload, CancellationToken cancellationToken)
        {
            if (!absoluteUrl.IsAbsoluteUri)
            {
                throw new ArgumentException();
            }
            var req = new HttpRequestMessage(method, absoluteUrl);
            if (payload != null)
            {
                req.Content = new StringContent(JsonConvert.SerializeObject(payload), Encoding.UTF8, "application/json");
            }
            return _http.SendAsync(req, cancellationToken);
        }

        public async Task AppendAsync(IEnumerable<UncommittedEvent> uncommitted, CancellationToken cancellationToken = default(CancellationToken))
        {
            var relativeUrl = "/api/event-store/" + Uri.EscapeDataString(Collection) + "/append";

            using (var res = await SendAsync(HttpMethod.Post, relativeUrl, uncommitted, cancellationToken))
            {
                if (res.IsSuccessStatusCode)
                {
                    return;
                }
                throw new CloudEventStoreClientException(res.StatusCode, res.ReasonPhrase);
            }
        }

        public async Task<CloudEventSegment> GetLogAsync(long offset, int take = 1000, CancellationToken cancellationToken = default(CancellationToken))
        {
            var relativeUrl = "/api/event-store/" + Uri.EscapeDataString(Collection) + "/log?offset=" + Convert.ToString(offset, CultureInfo.InvariantCulture) + "&take=" + Convert.ToString(take, CultureInfo.InvariantCulture);

            using (var res = await SendAsync(HttpMethod.Get, relativeUrl, null, cancellationToken))
            {
                if (res.IsSuccessStatusCode)
                {
                    var json = await res.Content.ReadAsStringAsync();
                    var result = JsonConvert.DeserializeObject<CloudEventSegment>(json);
                    return result;
                }
                throw new CloudEventStoreClientException(res.StatusCode, res.ReasonPhrase);
            }
        }

        public async Task<CloudEventSegment> GetStreamAsync(Guid streamId, int sequenceNumber, int take = 1000, CancellationToken cancellationToken = default(CancellationToken))
        {
            var relativeUrl = "/api/event-store/" + Uri.EscapeDataString(Collection) + "/stream/" + streamId.ToString() + "?offset=" + Convert.ToString(sequenceNumber, CultureInfo.InvariantCulture) + "&take=" + Convert.ToString(take, CultureInfo.InvariantCulture);

            using (var res = await SendAsync(HttpMethod.Get, relativeUrl, null, cancellationToken))
            {
                if (res.IsSuccessStatusCode)
                {
                    var json = await res.Content.ReadAsStringAsync();
                    var result = JsonConvert.DeserializeObject<CloudEventSegment>(json);
                    return result;
                }
                throw new CloudEventStoreClientException(res.StatusCode, res.ReasonPhrase);
            }
        }

        public async Task<CloudEventSegment> GetNextAsync(Uri next, CancellationToken cancellationToken = default(CancellationToken))
        {
            using (var res = await SendAsync(HttpMethod.Get, next, null, cancellationToken))
            {
                if (res.IsSuccessStatusCode)
                {
                    var json = await res.Content.ReadAsStringAsync();
                    var result = JsonConvert.DeserializeObject<CloudEventSegment>(json);
                    return result;
                }
                throw new CloudEventStoreClientException(res.StatusCode, res.ReasonPhrase);
            }
        }
    }
}

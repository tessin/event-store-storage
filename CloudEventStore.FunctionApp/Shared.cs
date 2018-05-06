using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudEventStore
{
    static class Shared
    {
        private static readonly CloudEventStoreFactory _factory;

        static Shared()
        {
            _factory = new CloudEventStoreFactory();
            _factory.StorageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
        }

        private static readonly ConcurrentDictionary<string, CloudEventStore> _collections = new ConcurrentDictionary<string, CloudEventStore>(StringComparer.Ordinal);

        public static CloudEventStore Collection(string collectionName)
        {
            CloudEventStore client;

            _Create:
            if (!_collections.TryGetValue(collectionName, out client))
            {
                if (!_collections.TryAdd(collectionName, client = _factory.Create(collectionName)))
                {
                    goto _Create;
                }
            }

            return client;
        }
    }
}

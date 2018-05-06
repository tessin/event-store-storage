﻿using Microsoft.WindowsAzure.Storage;
using System;

namespace CloudEventStore
{
    public class CloudEventStoreClientFactory
    {
        public CloudStorageAccount StorageAccount { get; set; }

        public CloudEventStoreClient Create(string collectionName = "default")
        {
            if (StorageAccount == null)
            {
                throw new InvalidOperationException("storage account has not been set");
            }

            if (string.IsNullOrEmpty(collectionName))
            {
                throw new ArgumentException("collection name is null or empty", nameof(collectionName));
            }

            var blobClient = new CloudEventBlobClient(StorageAccount);
            blobClient.Configuration.ContainerName = "event-log" + "-" + collectionName;

            var tableClient = new CloudEventTableClient(StorageAccount);
            tableClient.Configuration.TableName = "EventStore" + char.ToUpperInvariant(collectionName[0]) + collectionName.Substring(1).ToLowerInvariant();

            var client = new CloudEventStoreClient(blobClient, tableClient);
            return client;
        }
    }
}

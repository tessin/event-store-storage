﻿using CloudEventStore;
using CloudEventStore.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudEventStore
{
    [TestClass]
    public abstract class CloudStorageAccountTestBase
    {
        private const string ContainerPrefix = "test-";
        private const string TablePrefix = "testz";

        protected CloudStorageAccount StorageAccount { get; private set; }

        protected class TestConfiguration
        {
            public bool Dump { get; set; } = true;
        }

        protected TestConfiguration Configuration { get; private set; }

        [TestInitialize]
        public void TestInitialize()
        {
            var fn = Path.GetFullPath(@"..\..\..\..\storage-account.txt");
            var connectionString = File.ReadAllText(fn);
            connectionString = connectionString.Replace("DefaultEndpointsProtocol=https", "DefaultEndpointsProtocol=http"); // HTTP for debugger
            StorageAccount = CloudStorageAccount.Parse(connectionString);

            Delete();

            Configuration = new TestConfiguration();
        }

        protected static string GetContainerName()
        {
            return ContainerPrefix + Guid.NewGuid().ToString().Substring(0, 8);
        }

        protected async Task<CloudBlobContainer> GetContainerAsync()
        {
            var blobClient = StorageAccount.CreateCloudBlobClient();
            var container = blobClient.GetContainerReference(GetContainerName());
            await container.CreateIfNotExistsAsync();
            return container;
        }

        protected static string GetTableName()
        {
            return TablePrefix + Guid.NewGuid().ToString().Substring(0, 8);
        }

        protected async Task<CloudTable> GetTable()
        {
            var tableClient = StorageAccount.CreateCloudTableClient();
            var table = tableClient.GetTableReference(GetTableName());
            await table.CreateIfNotExistsAsync();
            return table;
        }

        protected void Delete(bool dump = false)
        {
            var blobClient = StorageAccount.CreateCloudBlobClient();
            foreach (var container in blobClient.ListContainers(ContainerPrefix))
            {
                if (dump)
                {
                    var mem = new MemoryStream();

                    foreach (var blob in container.ListBlobs().OfType<CloudBlob>())
                    {
                        mem.SetLength(0);
                        blob.DownloadToStream(mem);
                        mem.Position = 0;

                        Trace.WriteLine(blob.Name, "Blob");
                        Trace.WriteLine(mem.HexDump());
                    }
                }

                container.Delete();
            }

            var tableClient = StorageAccount.CreateCloudTableClient();
            foreach (var table in tableClient.ListTables(TablePrefix))
            {
                if (dump)
                {
                    var sb = new StringBuilder();

                    foreach (var entity in table.CreateQuery<DynamicTableEntity>())
                    {
                        sb.Length = 0;

                        sb.Append(entity.PartitionKey);
                        sb.Append(',');
                        sb.Append(entity.RowKey);
                        sb.Append(' ');

                        var i = 0;
                        foreach (var prop in entity.Properties)
                        {
                            if (0 < i)
                            {
                                sb.Append(',').Append(' ');
                            }

                            sb.Append(prop.Key);
                            sb.Append('=');
                            sb.Append(JsonConvert.SerializeObject(prop.Value.PropertyAsObject));

                            i++;
                        }

                        Trace.WriteLine(sb.ToString(), "Table");
                    }
                }

                table.Delete();
            }
        }

        [TestCleanup]
        public void TestCleanup()
        {
            Delete(Configuration.Dump);
        }
    }
}

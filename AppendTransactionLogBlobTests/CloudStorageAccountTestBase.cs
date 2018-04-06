using AppendBlobEventStore;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppendTransactionLogBlobTests
{
    [TestClass]
    public abstract class CloudStorageAccountTestBase
    {
        private CloudStorageAccount _storageAccount;
        private List<string> _containers;
        private List<string> _tables;

        public IFormatProvider CultreInfo { get; private set; }

        [TestInitialize]
        public void TestInitialize()
        {
            var fn = Path.GetFullPath(@"..\..\..\storage-account.txt");
            var connectionString = File.ReadAllText(fn);
            _storageAccount = CloudStorageAccount.Parse(connectionString);
        }

        protected async Task<CloudBlobContainer> GetBlobContainer()
        {
            var blobClient = _storageAccount.CreateCloudBlobClient();
            var container = blobClient.GetContainerReference("test-" + Guid.NewGuid().ToString().Substring(0, 8));
            await container.CreateIfNotExistsAsync();
            if (_containers == null)
            {
                _containers = new List<string>();
            }
            _containers.Add(container.Name);
            return container;
        }

        protected async Task<CloudTable> GetTable()
        {
            var tableClient = _storageAccount.CreateCloudTableClient();
            var table = tableClient.GetTableReference("TEST" + Guid.NewGuid().ToString().Substring(0, 8));
            await table.CreateIfNotExistsAsync();
            if (_tables == null)
            {
                _tables = new List<string>();
            }
            _tables.Add(table.Name);
            return table;
        }

        protected async Task<EventStoreStorage> GetEventStorage()
        {
            var task1 = GetBlobContainer();
            var task2 = GetTable();
            var eventStorage = new EventStoreStorage(_storageAccount, (await task1).Name, (await task2).Name);
            return eventStorage;
        }

        [TestCleanup]
        public void TestCleanup()
        {
            if (_containers != null)
            {
                var blobClient = _storageAccount.CreateCloudBlobClient();

                foreach (var name in _containers)
                {
                    var container = blobClient.GetContainerReference(name);

                    var mem = new MemoryStream();

                    foreach (var blob in container.ListBlobs().OfType<CloudBlob>())
                    {
                        mem.SetLength(0);
                        blob.DownloadToStream(mem);
                        mem.Position = 0;

                        Trace.WriteLine(blob.Name, "Blob");

                        DumpHex(mem);
                    }
                }

                _containers.Clear();
            }

            if (_tables != null)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();

                foreach (var name in _tables)
                {
                    var table = tableClient.GetTableReference(name);

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
                            sb.Append(Convert.ToString(prop.Value.PropertyAsObject, System.Globalization.CultureInfo.InvariantCulture));

                            i++;
                        }

                        Trace.WriteLine(sb.ToString(), "Table");
                    }

                    table.Delete();
                }

                _tables.Clear();
            }
        }

        private static void DumpHex(MemoryStream mem)
        {
            ArraySegment<byte> buf;
            mem.TryGetBuffer(out buf);

            var sb = new StringBuilder();

            var offsetFormat = "X8";

            if (mem.Length < (1 << 16))
            {
                offsetFormat = "X4";
            }

            int offset = 0;

            for (int i = 0; i + 16 < buf.Count; i += 16)
            {
                sb.Append(offset.ToString(offsetFormat));

                sb.Append(' ');

                DumpHex(buf.Array, buf.Offset + i, 16, sb);

                sb.Append(' ');
                sb.Append(Encoding.ASCII.GetString(buf.Array, buf.Offset + i, 16));

                sb.AppendLine();

                offset += 16;
            }

            var n = buf.Count % 16;
            if (0 < n)
            {
                sb.Append(offset.ToString(offsetFormat));

                sb.Append(' ');

                DumpHex(buf.Array, buf.Offset + buf.Count - n, n, sb);
                sb.Append(' ', 2 * (16 - n));

                sb.Append(Encoding.ASCII.GetString(buf.Array, buf.Offset + buf.Count - n, n));
            }

            Trace.WriteLine(sb);
        }

        private static void DumpHex(byte[] bytes, int offset, int count, StringBuilder sb)
        {
            for (int i = offset, end = offset + count; i < end; i++)
            {
                DumpHex(bytes[i] >> 4, sb);
                DumpHex(bytes[i] & 15, sb);
            }
        }

        private static void DumpHex(int nibble, StringBuilder sb)
        {
            if (nibble < 10)
            {
                sb.Append((char)('0' + nibble));
                return;
            }
            sb.Append((char)(('A' - 10) + nibble));
        }
    }
}

using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Threading.Tasks;

namespace CloudEventStore
{
    [TestClass]
    public class CloudEventStoreClientTests : CloudStorageAccountTestBase
    {
        [TestMethod]
        public async Task CloudEventStoreClient_AppendAsync_Test()
        {
            var containerName = GetContainerName();

            var client = new CloudEventStoreClient(StorageAccount);
            client.Configuration.ContainerName = containerName;
            await client.AppendAsync(new MemoryStream(new byte[] { 2 }));

            var client2 = new CloudEventStoreClient(StorageAccount);
            client2.Configuration.ContainerName = containerName;
            await client2.AppendAsync(new MemoryStream(new byte[] { 3 }));

            var client3 = new CloudEventStoreClient(StorageAccount);
            client3.Configuration.ContainerName = containerName;
            client3.Configuration.MaxCommittedBlockCount = 2;
            await client3.AppendAsync(new MemoryStream(new byte[] { 5 }));
            await client3.AppendAsync(new MemoryStream(new byte[] { 7 }));
            await client3.AppendAsync(new MemoryStream(new byte[] { 11 }));
        }
    }
}

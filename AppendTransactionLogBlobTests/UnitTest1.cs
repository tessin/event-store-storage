using System;
using System.Threading.Tasks;
using AppendBlobEventStore;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace AppendTransactionLogBlobTests
{
    [TestClass]
    public class UnitTest1 : CloudStorageAccountTestBase
    {
        [TestMethod]
        public async Task TestMethod1()
        {
            var storage = await GetEventStorage();

            var store = new EventStore(storage);

            await store.AppendAsync(new[] { new UncomittedEvent { StreamId = Guid.NewGuid(), SequenceNumber = 1, TypeId = Guid.NewGuid(), Payload = new byte[0], Created = DateTimeOffset.UtcNow } });

            await store.GetEventSegmented(0);
        }
    }
}

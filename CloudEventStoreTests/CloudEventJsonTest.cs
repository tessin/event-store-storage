using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudEventStore
{
    [TestClass]
    public class CloudEventJsonTest
    {
        [TestMethod]
        public void CloudEvent_Json_Test()
        {
            var e = new CloudEvent
            {
                Id = 1,
                StreamId = Guid.NewGuid(),
                SequenceNumber = 1,
                TypeId = Guid.NewGuid(),
                Created = DateTimeOffset.UtcNow,
                Payload = new ArraySegment<byte>(new byte[] { 2, 3, 5, 7, 11 })
            };

            var json = JsonConvert.SerializeObject(e);

            var e2 = JsonConvert.DeserializeObject<CloudEvent>(json);

            Assert.AreEqual(e.Id, e2.Id);
            Assert.AreEqual(e.StreamId, e2.StreamId);
            Assert.AreEqual(e.SequenceNumber, e2.SequenceNumber);
            Assert.AreEqual(e.TypeId, e2.TypeId);
            Assert.AreEqual(e.Created, e2.Created);
            CollectionAssert.AreEqual(e.Payload.ToArray(), e2.Payload.ToArray());
        }
    }
}

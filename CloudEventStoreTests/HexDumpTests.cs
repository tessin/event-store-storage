using System;
using System.IO;
using CloudEventStore.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudEventStore
{
    [TestClass, TestCategory("HexDump")]
    public class HexDumpTests
    {
        [TestMethod]
        public void HexDumpTest()
        {
            var bytes = new byte[] {
                0, 0, 0, 0,
                0, 0, 0, 0,
                0, 0, 0, 0,
                0, 0, 0, 0,
            };

            var hexDump = new MemoryStream(bytes).HexDump();

            Assert.AreEqual("0000 00000000000000000000000000000000 ................" + Environment.NewLine, hexDump);
        }
    }
}

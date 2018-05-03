using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using CloudEventStore.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudEventStore
{
    [TestClass, TestCategory("VarInt63")]
    public class BinaryUtilsTests
    {
        [TestMethod]
        [DataRow(new byte[] { 0 }, 0L)]
        [DataRow(new byte[] { 1 }, 1L)]
        [DataRow(new byte[] { 0x7F }, 127L)]
        [DataRow(new byte[] { 0x81, 0x00 }, 128L)]
        public void BinaryUtils_WriteVarInt63_Test(byte[] expected, long v)
        {
            var mem = new MemoryStream();
            mem.WriteVarInt63(v);
            CollectionAssert.AreEqual(expected, mem.ToArray());
        }

        [TestMethod, ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void BinaryUtils_WriteVarInt63_ArgumentOutOfRangeException_Test()
        {
            var mem = new MemoryStream();
            mem.WriteVarInt63(-1);
        }

        [TestMethod]
        [DataRow(0L, new byte[] { 0 })]
        [DataRow(1L, new byte[] { 1 })]
        [DataRow(127L, new byte[] { 0x7F })]
        [DataRow(128L, new byte[] { 0x81, 0x00 })]
        public void BinaryUtils_ReadVarInt63_Test(long expected, byte[] v)
        {
            var mem = new MemoryStream(v);
            Assert.AreEqual(expected, mem.ReadVarInt63());
        }

        [TestMethod]
        public void BinaryUtils_ReadVarInt63_OverflowException_Test1()
        {
            var mem = new MemoryStream(new byte[] { 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00 });
            Assert.AreEqual(0L, mem.ReadVarInt63());
        }

        [TestMethod, ExpectedException(typeof(OverflowException))]
        public void BinaryUtils_ReadVarInt63_OverflowException_Test2()
        {
            var mem = new MemoryStream(new byte[] { 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80 });
            mem.ReadVarInt63();
        }

        [TestMethod, ExpectedException(typeof(EndOfStreamException))]
        public void BinaryUtils_ReadVarInt63_EndOfStreamException_Test()
        {
            var mem = new MemoryStream(new byte[0]);
            mem.ReadVarInt63();
        }

        [TestMethod]
        public void BinaryUtils_VarInt63_Test()
        {
            var r = new Random();

            var m = new MemoryStream();
            for (int i = 0; i < 1000; i++)
            {
                var v = (long)((1UL << (7 * (r.Next(9) + 1))) * r.NextDouble());
                m.SetLength(0);
                m.WriteVarInt63(v);
                m.Position = 0;
                var x = m.ReadVarInt63();
                Assert.AreEqual(v, x);
            }
        }
    }
}

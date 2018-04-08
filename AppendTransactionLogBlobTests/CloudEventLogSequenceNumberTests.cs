﻿using System;
using AppendTransactionLogBlob;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace AppendTransactionLogBlob
{
    [TestClass]
    public class CloudEventLogSequenceNumberTests
    {
        [TestMethod]
        [DataRow(0, 0)]
        [DataRow(0, (1L << 38) - 1)]
        [DataRow(1, 0)]
        [DataRow(1, (1L << 38) - 1)]
        [DataRow((1 << 25) - 1, 0)]
        [DataRow((1 << 25) - 1, (1L << 38) - 1)]
        public void TestMethod1(long hi, long lo)
        {
            var a = new CloudEventLogSequenceNumber(hi, lo);

            Assert.AreEqual(hi, a.Log);
            Assert.AreEqual(lo, a.SequenceNumber);

            var v = a.Value;
            var b = new CloudEventLogSequenceNumber(v);

            Assert.AreEqual(hi, b.Log);
            Assert.AreEqual(lo, b.SequenceNumber);
        }
    }
}

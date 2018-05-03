using System;
using System.Collections.Generic;
using CloudEventStore.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudEventStore
{
    [TestClass]
    public class StringUtilsTests
    {
        [TestMethod]
        [DataRow("1", 1L, 1)]
        [DataRow("01", 1L, 2)]
        [DataRow("001", 1L, 3)]
        public void StringUtils_ToFixed_Test(string expected, long v, int n)
        {
            Assert.AreEqual(expected, v.ToFixed(n));
        }

        [TestMethod, ExpectedException(typeof(ArgumentOutOfRangeException))]
        [DataRow("", -1L, 3)]
        [DataRow("", 1000L, 3)]
        public void StringUtils_ToFixed_ArgumentOutOfRangeException_Test(string expected, long v, int n)
        {
            Assert.AreEqual(expected, v.ToFixed(n));
        }

        [TestMethod]
        [DataRow("0000000000000", new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 })]
        [DataRow("zzzzzzzzzzzzf", new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
        public void StringUtils_Encode32_Test(string expected, byte[] v)
        {
            Assert.AreEqual(expected, v.Encode32());
        }

        [TestMethod]
        public void StringUtils_Encode32_Lexicographic_Test()
        {
            var r = new Random();

            var xs = new List<ulong>();
            var ys = new List<string>();

            for (int i = 0; i < 100; i++)
            {
                var v = (ulong)(ulong.MaxValue * r.NextDouble());

                xs.Add(v);
                ys.Add(BitConverter.GetBytes(v).Encode32());
            }

            xs.Sort();
            ys.Sort(StringComparer.Ordinal);

            for (int i = 0; i < 100; i++)
            {
                var x = xs[i];
                var y = ys[i];

                var s = BitConverter.GetBytes(x).Encode32();

                Assert.AreEqual(y, s);
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppendTransactionLogBlob.Internal
{
    public static class CryptoUtils
    {
        private static System.Security.Cryptography.RNGCryptoServiceProvider RNG = new System.Security.Cryptography.RNGCryptoServiceProvider();

        public static byte[] GetBytes(int n)
        {
            var bytes = new byte[n];
            RNG.GetBytes(bytes);
            return bytes;
        }
    }
}

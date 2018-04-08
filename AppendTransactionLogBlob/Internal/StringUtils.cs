using System;
using System.Text;

namespace AppendTransactionLogBlob.Internal
{
    // internal
    public static class StringUtils
    {
        public static string ToFixed(this long v, int n, char paddingChar = '0')
        {
            if (!(0 <= v)) throw new ArgumentOutOfRangeException(nameof(v));

            var xs = new char[n];

            int i = 0;
            while ((v != 0) & (i < n))
            {
                var x = (int)(v % 10);
                v /= 10;
                xs[n - (i + 1)] = (char)('0' + x);
                i++;
            }
            for (; i < n; i++)
            {
                xs[n - (i + 1)] = paddingChar;
            }
            if (v != 0)
            {
                throw new ArgumentOutOfRangeException(nameof(v));
            }

            return new string(xs);
        }

        public static long FromFixed(this string s, int offset, int length)
        {
            long v = 0;

            //  9223372036854775807
            // 01234567890123456789
            //           1111111111

            for (int i = 0; i < length; i++)
            {
                var ch = s[offset + i];
                if (('0' <= ch) & (ch <= '9'))
                {
                    var d = ch - (long)'0';
                    v *= 10;
                    v += d;
                }
                else
                {
                    throw new ArgumentException("illegal character in string", nameof(s));
                }
            }

            return v;
        }

        public static string Encode32(this byte[] bytes)
        {
            return Encode32(bytes, 0, bytes.Length);
        }

        public static string Encode32(this byte[] bytes, int offset)
        {
            return Encode32(bytes, offset, bytes.Length - offset);
        }

        public static string Encode32(this byte[] bytes, int offset, int count)
        {
            var sb = new StringBuilder();
            Encode32(bytes, offset, count, sb);
            return sb.ToString();
        }

        private static void Encode32(byte[] bytes, int offset, int count, StringBuilder sb)
        {
            if (!((count & 7) == 0)) throw new ArgumentOutOfRangeException(nameof(count));

            for (int i = 0; i < count; i += 8)
            {
                Encode32(BitConverter.ToUInt64(bytes, offset + i), sb);
            }
        }

        private static readonly string _crockford32 = "0123456789abcdefghjkmnpqrstvwxyz";

        private static void Encode32(ulong v, StringBuilder sb)
        {
            var s = _crockford32;
            var m = (ulong)s.Length - 1;
            sb.Append(s[(int)((v >> 60) & m)]);
            sb.Append(s[(int)((v >> 55) & m)]);
            sb.Append(s[(int)((v >> 50) & m)]);
            sb.Append(s[(int)((v >> 45) & m)]);
            sb.Append(s[(int)((v >> 40) & m)]);
            sb.Append(s[(int)((v >> 35) & m)]);
            sb.Append(s[(int)((v >> 30) & m)]);
            sb.Append(s[(int)((v >> 25) & m)]);
            sb.Append(s[(int)((v >> 20) & m)]);
            sb.Append(s[(int)((v >> 15) & m)]);
            sb.Append(s[(int)((v >> 10) & m)]);
            sb.Append(s[(int)((v >> 5) & m)]);
            sb.Append(s[(int)(v & m)]);
        }
    }
}

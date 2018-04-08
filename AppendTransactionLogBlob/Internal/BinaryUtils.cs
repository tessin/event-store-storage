using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppendTransactionLogBlob.Internal
{
    public static class BinaryUtils
    {
        public static int VarInt63Size(long v)
        {
            if (!(0 <= v)) throw new ArgumentOutOfRangeException(nameof(v));

            if (v <= (1L << 7) - 1) // 1 byte
            {
                return 1; // byte
            }

            if (v <= (1L << 14) - 1)
            {
                return 2; // bytes
            }

            if (v <= (1L << 21) - 1)
            {
                return 3; // bytes
            }

            if (v <= (1L << 28) - 1)
            {
                return 4; // bytes
            }

            if (v <= (1L << 35) - 1)
            {
                return 4; // bytes
            }

            if (v <= (1L << 42) - 1)
            {
                return 6; // bytes
            }

            if (v <= (1L << 49) - 1)
            {
                return 7; // bytes
            }

            if (v <= (1L << 56) - 1)
            {
                return 8; // bytes
            }

            return 9; // bytes
        }

        public static void WriteVarInt63(this Stream outputStream, long v)
        {
            if (!(0 <= v)) throw new ArgumentOutOfRangeException(nameof(v));

            if (v <= (1L << 7) - 1) // 1 byte
            {
                outputStream.WriteByte((byte)v);
                return;
            }

            if (v <= (1L << 14) - 1) // 2 bytes 
            {
                outputStream.WriteByte((byte)((v >> 7) | 0x80));
                outputStream.WriteByte((byte)(v & 0x7F));
                return;
            }

            if (v <= (1L << 21) - 1) // 3 bytes 
            {
                outputStream.WriteByte((byte)((v >> 14) | 0x80));
                outputStream.WriteByte((byte)((v >> 7) | 0x80));
                outputStream.WriteByte((byte)(v & 0x7F));
                return;
            }

            if (v <= (1L << 28) - 1) // 4 bytes 
            {
                outputStream.WriteByte((byte)((v >> 21) | 0x80));
                outputStream.WriteByte((byte)((v >> 14) | 0x80));
                outputStream.WriteByte((byte)((v >> 7) | 0x80));
                outputStream.WriteByte((byte)(v & 0x7F));
                return;
            }

            if (v <= (1L << 35) - 1) // 5 bytes 
            {
                outputStream.WriteByte((byte)((v >> 28) | 0x80));
                outputStream.WriteByte((byte)((v >> 21) | 0x80));
                outputStream.WriteByte((byte)((v >> 14) | 0x80));
                outputStream.WriteByte((byte)((v >> 7) | 0x80));
                outputStream.WriteByte((byte)(v & 0x7F));
                return;
            }

            if (v <= (1L << 42) - 1) // 6 bytes 
            {
                outputStream.WriteByte((byte)((v >> 35) | 0x80));
                outputStream.WriteByte((byte)((v >> 28) | 0x80));
                outputStream.WriteByte((byte)((v >> 21) | 0x80));
                outputStream.WriteByte((byte)((v >> 14) | 0x80));
                outputStream.WriteByte((byte)((v >> 7) | 0x80));
                outputStream.WriteByte((byte)(v & 0x7F));
                return;
            }

            if (v <= (1L << 49) - 1) // 7 bytes 
            {
                outputStream.WriteByte((byte)((v >> 42) | 0x80));
                outputStream.WriteByte((byte)((v >> 35) | 0x80));
                outputStream.WriteByte((byte)((v >> 28) | 0x80));
                outputStream.WriteByte((byte)((v >> 21) | 0x80));
                outputStream.WriteByte((byte)((v >> 14) | 0x80));
                outputStream.WriteByte((byte)((v >> 7) | 0x80));
                outputStream.WriteByte((byte)(v & 0x7F));
                return;
            }

            if (v <= (1L << 56) - 1) // 8 bytes 
            {
                outputStream.WriteByte((byte)((v >> 49) | 0x80));
                outputStream.WriteByte((byte)((v >> 42) | 0x80));
                outputStream.WriteByte((byte)((v >> 35) | 0x80));
                outputStream.WriteByte((byte)((v >> 28) | 0x80));
                outputStream.WriteByte((byte)((v >> 21) | 0x80));
                outputStream.WriteByte((byte)((v >> 14) | 0x80));
                outputStream.WriteByte((byte)((v >> 7) | 0x80));
                outputStream.WriteByte((byte)(v & 0x7F));
                return;
            }

            // 9 bytes 
            outputStream.WriteByte((byte)((v >> 56) | 0x80));
            outputStream.WriteByte((byte)((v >> 49) | 0x80));
            outputStream.WriteByte((byte)((v >> 42) | 0x80));
            outputStream.WriteByte((byte)((v >> 35) | 0x80));
            outputStream.WriteByte((byte)((v >> 28) | 0x80));
            outputStream.WriteByte((byte)((v >> 21) | 0x80));
            outputStream.WriteByte((byte)((v >> 14) | 0x80));
            outputStream.WriteByte((byte)((v >> 7) | 0x80));
            outputStream.WriteByte((byte)(v & 0x7F));
        }

        public static long ReadVarInt63(this Stream inputStream)
        {
            int n = 0;

            long v = 0;
            _ReadByte:
            {
                if (!(n < 9)) throw new OverflowException();
                var b = inputStream.ReadByte();
                if (b == -1) throw new EndOfStreamException();
#pragma warning disable CS0675 // Bitwise-or operator used on a sign-extended operand
                v = (v << 7) | (b & 0x7F);
#pragma warning restore CS0675
                if ((b & 0x80) == 0x80)
                {
                    n++;
                    goto _ReadByte;
                }
            }

            return v;
        }

        /// <summary>
        /// Create a hex dump of the contents of the memory stream, regardless of Position, note that Position is not changed, the bytes are not actually read from the MemoryStream.
        /// </summary>
        public static string HexDump(this MemoryStream mem)
        {
            if (mem.TryGetBuffer(out var buffer))
            {
                return HexDump(buffer.Array, buffer.Offset, buffer.Count);
            }
            else
            {
                return HexDump(mem.ToArray());
            }
        }

        public static string HexDump(byte[] buf)
        {
            return HexDump(buf, 0, buf.Length);
        }

        public static string HexDump(byte[] buf, int offset)
        {
            return HexDump(buf, offset, buf.Length - offset);
        }

        public static string HexDump(byte[] buf, int offset, int count)
        {
            var sb = new StringBuilder();

            var offsetFormat = "X8";

            if (count < (1 << 16))
            {
                offsetFormat = "X4";
            }

            int addr = 0;

            for (int i = 0; i + 16 <= count; i += 16)
            {
                sb.Append(addr.ToString(offsetFormat));

                sb.Append(' ');

                HexDumpHex(buf, offset + i, 16, sb);

                sb.Append(' ');

                HexDumpAscii(buf, offset + i, 16, sb);

                sb.AppendLine();

                addr += 16;
            }

            var n = count % 16;
            if (0 < n)
            {
                sb.Append(addr.ToString(offsetFormat));

                sb.Append(' ');

                HexDumpHex(buf, offset + count - n, n, sb);
                sb.Append(' ', 2 * (16 - n));

                sb.Append(' ');

                HexDumpAscii(buf, offset + count - n, n, sb);
            }

            return sb.ToString();
        }

        private static void HexDumpHex(byte[] bytes, int offset, int count, StringBuilder sb)
        {
            for (int i = offset, end = offset + count; i < end; i++)
            {
                HexDumpHexNibble(bytes[i] >> 4, sb);
                HexDumpHexNibble(bytes[i] & 15, sb);
            }
        }

        private static void HexDumpHexNibble(int nibble, StringBuilder sb)
        {
            if (nibble < 10)
            {
                sb.Append((char)('0' + nibble));
                return;
            }
            sb.Append((char)(('A' - 10) + nibble));
        }

        private static void HexDumpAscii(byte[] bytes, int offset, int count, StringBuilder sb)
        {
            for (int i = offset, end = offset + count; i < end; i++)
            {
                var b = bytes[i];
                if ((0x20 <= b) & (b <= 0x7E))
                {
                    sb.Append((char)b);
                }
                else
                {
                    sb.Append('.'); // mask
                }
            }
        }
    }
}

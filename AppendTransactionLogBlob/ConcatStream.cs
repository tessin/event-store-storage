using System;
using System.IO;

namespace AppendTransactionLogBlob
{
    class ConcatStream : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => false;

        public override long Length => _first.Length + _second.Length;
        public override long Position { get => _first.Position + _second.Position; set => throw new NotSupportedException(); }

        private readonly Stream _first;
        private readonly Stream _second;
        private int _state;

        public ConcatStream(Stream first, Stream second)
        {
            _first = first;
            _second = second;
            _state = 0;
        }

        public override void Flush()
        {
            throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int offset0 = offset;
            _MoveNext:
            switch (_state)
            {
                case 0:
                    {
                        var read = _first.Read(buffer, offset, count);
                        if (read <= count)
                        {
                            offset += read;
                            count -= read;
                            _state = 1;
                            goto _MoveNext;
                        }
                    }
                    break;
                case 1:
                    {
                        var read = _second.Read(buffer, offset, count);
                        if (read <= count)
                        {
                            offset += read;
                            count -= read;
                            _state = 2;
                            goto _MoveNext;
                        }
                    }
                    break;
            }
            return offset - offset0;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }
    }
}

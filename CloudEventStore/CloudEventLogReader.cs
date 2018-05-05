using CloudEventStore.Internal;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudEventStore
{
    public class CloudEventLogReader : IDisposable
    {
        private Stream _logStream;
        private readonly long _log;
        private readonly long _end;

        public CloudEventLogPosition Position => new CloudEventLogPosition(_log, _logStream.Position);

        public CloudEventLogReader(Stream logStream, long log, long end)
        {
            this._logStream = logStream;
            this._log = log;
            this._end = end;
        }

        private CloudEvent _current;
        public CloudEvent Current => _current;

        public async Task<bool> ReadAsync()
        {
            var pos = _logStream.Position;
            if (pos < _end)
            {
                var size = _logStream.ReadVarInt63();
                var bytes = new byte[size];
                if (await _logStream.ReadAsync(bytes, 0, bytes.Length) != bytes.Length)
                {
                    throw new InvalidOperationException("undefined behavior");
                }
                _current = new CloudEvent(new CloudEventLogPosition(_log, pos), new ArraySegment<byte>(bytes));
                return true;
            }
            _current = null;
            return false;
        }

        public void Dispose()
        {
            _logStream.Dispose();
        }
    }
}

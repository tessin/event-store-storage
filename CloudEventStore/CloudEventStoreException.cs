using System;
using System.Runtime.Serialization;

namespace CloudEventStore
{
    [Serializable]
    public enum CloudEventStoreError
    {
        None,
        BatchLimit,
    }

    [Serializable]
    public class CloudEventStoreException : Exception
    {
        public CloudEventStoreError Error { get; }

        public CloudEventStoreException(CloudEventStoreError error, string message = null, Exception inner = null)
            : base(message, inner)
        {
            Error = error;
        }

        protected CloudEventStoreException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            Error = (CloudEventStoreError)info.GetValue(nameof(Error), typeof(CloudEventStoreError));
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue(nameof(Error), Error, typeof(CloudEventStoreError));

            base.GetObjectData(info, context);
        }
    }
}

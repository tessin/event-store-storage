using System;

namespace CloudEventStore
{
    static class ArrayUtils
    {
        public static T GetItem<T>(this ArraySegment<T> segment, int index)
        {
            return segment.Array[segment.Offset + index];
        }
    }
}

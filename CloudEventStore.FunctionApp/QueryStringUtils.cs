using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudEventStore
{
    static class QueryStringUtils
    {
        public static T GetValueOrDefault<T>(this IEnumerable<KeyValuePair<string, string>> source, string name, T defaultValue = default(T))
        {
            foreach (var item in source)
            {
                if (item.Key == name)
                {
                    return (T)Convert.ChangeType(item.Value, typeof(T), CultureInfo.InvariantCulture);
                }
            }
            return defaultValue;
        }
    }
}

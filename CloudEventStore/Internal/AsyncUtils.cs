using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudEventStore.Internal
{
    public static class AsyncUtils
    {
        public static async Task ForEachAsync<T>(this IEnumerable<T> source, Func<T, Task> taskFactory, int? maxDegreeOfConcurrency = null)
        {
            var maxDegreeOfConcurrency2 = maxDegreeOfConcurrency ?? 2 * Environment.ProcessorCount;

            var tasks = new List<Task>();

            using (var it = source.GetEnumerator())
            {
                for (; ; )
                {
                    while (tasks.Count < maxDegreeOfConcurrency2 && it.MoveNext())
                    {
                        tasks.Add(taskFactory(it.Current));
                    }

                    if (tasks.Count == 0)
                    {
                        break;
                    }

                    var task = await Task.WhenAny(tasks);
                    await task; // unwrap
                    tasks.Remove(task);
                }
            }
        }
    }
}

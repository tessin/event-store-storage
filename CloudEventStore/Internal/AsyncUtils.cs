using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace CloudEventStore.Internal
{
    public static class Async
    {
        public static AsyncLazy<T> Lazy<T>(Func<Task<T>> taskFactory)
        {
            return new AsyncLazy<T>(taskFactory);
        }
    }

    public class AsyncLazy<T>
    {
        private readonly Lazy<Task<T>> _lazy;

        public bool IsValueCreated => _lazy.IsValueCreated;

        public AsyncLazy(Func<Task<T>> taskFactory)
        {
            _lazy = new Lazy<Task<T>>(() => Task.Factory.StartNew(() => taskFactory()).Unwrap());
        }

        public TaskAwaiter<T> GetAwaiter()
        {
            return _lazy.Value.GetAwaiter();
        }
    }

    public static class AsyncUtils
    {
        public static async Task ForEachAsync<T>(this IEnumerable<T> source, Func<T, int, Task> taskFactory, int? maxDegreeOfConcurrency = null)
        {
            var maxDegreeOfConcurrency2 = maxDegreeOfConcurrency ?? 2 * Environment.ProcessorCount;

            var tasks = new List<Task>();

            using (var it = source.GetEnumerator())
            {
                for (int i = 0; ;)
                {
                    while (tasks.Count < maxDegreeOfConcurrency2 && it.MoveNext())
                    {
                        tasks.Add(taskFactory(it.Current, i++));
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

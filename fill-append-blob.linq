<Query Kind="Program">
  <NuGetReference Version="7.2.1">WindowsAzure.Storage</NuGetReference>
  <Namespace>Microsoft.WindowsAzure.Storage</Namespace>
  <Namespace>System.Threading.Tasks</Namespace>
  <Namespace>System.Net</Namespace>
</Query>

async Task Main()
{
	// note: this script can take up to 10 minutes to complete
	
	var storageAccount = CloudStorageAccount.Parse(File.ReadAllText(Path.Combine(Path.GetDirectoryName(Util.CurrentQueryPath), "storage-account.txt")));

	var blobClient = storageAccount.CreateCloudBlobClient();

	var blobServicePoint = ServicePointManager.FindServicePoint(blobClient.BaseUri);
	blobServicePoint.ConnectionLimit = 10;

	var limits = blobClient.GetContainerReference("limits");
	await limits.CreateIfNotExistsAsync();

	var appendBlobBlockCount = limits.GetAppendBlobReference("append-blob-block-count");

	await appendBlobBlockCount.CreateOrReplaceAsync();

	var t = Stopwatch.StartNew();
	var d = TimeSpan.FromSeconds(30);

	await AsyncUtils.ForEachAsync(
		Enumerable.Range(0, 50000),
		async (i) => await appendBlobBlockCount.AppendBlockAsync(new MemoryStream(BitConverter.GetBytes(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()))),
		20
	);
}

// Define other methods and classes here

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

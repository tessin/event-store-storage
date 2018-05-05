<Query Kind="Program">
  <NuGetReference Version="7.2.1">WindowsAzure.Storage</NuGetReference>
  <Namespace>System.Threading.Tasks</Namespace>
  <Namespace>Microsoft.WindowsAzure.Storage</Namespace>
  <Namespace>Microsoft.WindowsAzure.Storage.Blob</Namespace>
</Query>

async Task Main()
{
	var storageAccount = CloudStorageAccount.Parse(File.ReadAllText(Path.Combine(Path.GetDirectoryName(Util.CurrentQueryPath), "storage-account.txt")));

	var blobClient = storageAccount.CreateCloudBlobClient();

	var limits = blobClient.GetContainerReference("limits");
	await limits.CreateIfNotExistsAsync();

	var listSegment = await limits.ListBlobsSegmentedAsync(null, true, Microsoft.WindowsAzure.Storage.Blob.BlobListingDetails.None, 1, null, null, null);

	var appendBlobBlockCount = limits.GetAppendBlobReference("append-blob-block-count");

	try
	{
		await appendBlobBlockCount.AppendBlockAsync(new MemoryStream(BitConverter.GetBytes(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())));
	}
	catch (StorageException ex)
	{
		ex.Message.Dump();
		
		var w = new StringWriter();
		ex.RequestInformation.WriteXml(XmlWriter.Create(w));
		w.Dump();
	}
}
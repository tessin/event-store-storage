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
	
	var appendBlob = (CloudAppendBlob)listSegment.Results.First();
	await appendBlob.FetchAttributesAsync();
	appendBlob.Properties.Dump();

	//	var appendBlobBlockCount = limits.GetAppendBlobReference("append-blob-block-count");
	//	await appendBlobBlockCount.FetchAttributesAsync();
	//
	//	appendBlobBlockCount.Properties.Dump();
}



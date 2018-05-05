using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CloudEventStore.Internal
{
    static class BlobUtils
    {
        public static async Task<bool> CreateIfNotExistsAsync(this CloudAppendBlob appendBlob, CancellationToken cancellationToken = default(CancellationToken))
        {
            try
            {
                await appendBlob.CreateOrReplaceAsync(AccessCondition.GenerateIfNotExistsCondition(), null, null, cancellationToken);
            }
            catch (StorageException ex) when (ex.RequestInformation.HttpStatusCode == 409)
            {
                return false;
            }
            return true;
        }
    }
}

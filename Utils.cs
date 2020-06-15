using System;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureEventJournal
{
    public static class Utils
    {
        public static byte[] Zip(string str) {
            var bytes = Encoding.UTF8.GetBytes(str);
            using (var msi = new MemoryStream(bytes))
            using (var mso = new MemoryStream()) 
            {
                using (var gs = new GZipStream(mso, CompressionMode.Compress)) 
                {
                    msi.CopyTo(gs);
                }
                return mso.ToArray();
            }
        }

        public static string Unzip(byte[] bytes)
        {
            using (var msi = new MemoryStream(bytes))
            using (var mso = new MemoryStream())
            {
                using (var gs = new GZipStream(msi, CompressionMode.Decompress))
                {
                    gs.CopyTo(mso);
                }
                return Encoding.UTF8.GetString(mso.ToArray());
            }
        }

        public static async Task<Storage> GetStorageAsync(Binder binder, string collectionName)
        {
            var table = await binder.BindAsync<CloudTable>(new Attribute[] { new TableAttribute(collectionName) });
            var blobContainer = await binder.BindAsync<CloudBlobContainer>(new Attribute[] { new BlobAttribute(collectionName) });
            return await table.ExistsAsync() && await blobContainer.ExistsAsync()
                ? new Storage(table, blobContainer)
                : null;
        }
        public static void LogException(Exception ex, ILogger log)
        {
            log.LogError($"{ex.GetType()} ({ex.Message})");
            if (ex != ex.GetBaseException())
            {
                log.LogError($"{ex.GetBaseException().GetType()} ({ex.GetBaseException().Message})");
            }
        }
    }
}
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureEventJournal
{
    public class EventEntity : TableEntity
    {
        const int MaxTableContentLength = 0xFFFF;
        public const string PartitionKeyName = "PartitionKey";
        public const string RowKeyName = "RowKey";
        public const string IdKeyPrefix = "id:";
        public const string DateKeyPrefix = "date:";

        public string Id { get; set; }
        public string ProgramId { get; set; }
        public string CarrierId { get; set; }
        public string ServiceName { get; set; }
        public string Description { get; set; }
        public DateTime Created { get; set; }

        public EventEntity()
        {
        }

        public EventEntity(string entityId, DateTime created)
        {
            this.PartitionKey = CreatePartitionKey(entityId);
            this.RowKey = CreateRowKey(created);
        }

public static string CreatePartitionKey(string id)
{
    return IdKeyPrefix + NormalizeId(id);
}

public static string CreatePartitionKey(DateTime date)
{
    var utcDate = date.ToUniversalTime();
    return DateKeyPrefix + $"{utcDate.Year:D4}{utcDate.Month:D2}{utcDate.Day:D2}";
}

public static string CreateRowKeyBase(DateTime created)
{
    return $"{(DateTime.MaxValue - created.ToUniversalTime()).Ticks + 1:D19}";
}

public static string CreateRowKey(DateTime created)
{
    var keyBase = CreateRowKeyBase(created);
    var rowGuid = Guid.NewGuid().ToString("N").ToLower();
    return $"{keyBase}-{rowGuid}";
}

        public static string CreateContentId(string id, string rowKey)
        {
            return $"{NormalizeId(id)}-{rowKey}";
        }

        private string CreateContentId()
        {
            return CreateContentId(this.Id ?? this.ProgramId, this.RowKey);
        }

        private static string NormalizeId(string id)
        {
            return id.Replace("-", "").ToLower();
        }

        public static string CreateFilterForId(string id)
        {
            var partitionKey = CreatePartitionKey(id);
            return TableQuery.GenerateFilterCondition(PartitionKeyName, QueryComparisons.Equal, partitionKey);
        }

        public static string CreateFilterForKey(string partitionKey, string rowKey)
        {
            return CombineFilters(
                TableQuery.GenerateFilterCondition(PartitionKeyName, QueryComparisons.Equal, partitionKey),
                TableQuery.GenerateFilterCondition(RowKeyName, QueryComparisons.Equal, rowKey));
        }

        public static string CreateFilterForTimeInterval(DateTime startTime, DateTime endTime)
        {
            var startPartitionKey = CreatePartitionKey(startTime);
            var endPartitionKey = CreatePartitionKey(endTime == endTime.Date ? endTime.AddDays(-1) : endTime);
			// Time-based key index stores time values in descending order, so when building time interval filters
			// we must revert comparison, i.e. <= startKey and > endKey
            var combinedFilter = startPartitionKey == endPartitionKey
                ? TableQuery.GenerateFilterCondition(PartitionKeyName, QueryComparisons.Equal, startPartitionKey)
                : CombineFilters(
                    TableQuery.GenerateFilterCondition(PartitionKeyName, QueryComparisons.GreaterThanOrEqual, startPartitionKey),
                    TableQuery.GenerateFilterCondition(PartitionKeyName, QueryComparisons.LessThan, endPartitionKey));

            if (startTime != startTime.Date)
            {
                var rowKey = CreateRowKeyBase(startTime);
				// See the note above regarding that explains the choice of comparison operator
                var rowFilter = TableQuery.GenerateFilterCondition(RowKeyName, QueryComparisons.LessThanOrEqual, rowKey);
                combinedFilter = CombineFilters(combinedFilter, rowFilter);
            }

            if (endTime != endTime.Date)
            {
                var rowKey = CreateRowKeyBase(endTime);
				// See the note above regarding that explains the choice of comparison operator
                var rowFilter = TableQuery.GenerateFilterCondition(RowKeyName, QueryComparisons.GreaterThan, rowKey);
                combinedFilter = CombineFilters(combinedFilter, rowFilter);
            }

            return combinedFilter;
        }

        private static string CombineFilters(string filter1, string filter2)
        {
            return TableQuery.CombineFilters(filter1, TableOperators.And, filter2);
        }

        public byte[] Content { get; set; }
        public string ContentId => this.Content == null ? CreateContentId() : null;

        public static async Task<EventEntity> LoadFromStorageAsync(string partitionKey, string rowKey, 
            CloudTable table, CloudBlobContainer container, bool doNotLoadContent = false)
        {
            var cond1 = TableQuery.GenerateFilterCondition(PartitionKeyName, QueryComparisons.Equal, partitionKey);
            var cond2 = TableQuery.GenerateFilterCondition(RowKeyName, QueryComparisons.Equal, rowKey);
            var filter = TableQuery.CombineFilters(cond1, TableOperators.And, cond2);

            var query = new TableQuery<EventEntity>().Where(filter);
            var seg = await table.ExecuteQuerySegmentedAsync(query, null);
            var result = seg.FirstOrDefault();
            if (result != null && !doNotLoadContent)
            {
                await result.LoadContentAsync(table, container);
            }
            return result;
        }

        public async Task<EventEntity> LoadFromStorageAsync(string contentId, CloudTable table, CloudBlobContainer container, bool doNotLoadContent = false)
        {
            var id = contentId.Split('-').First();
            var partitionKey = CreatePartitionKey(id);
            var rowKey = contentId.Substring(id.Length + 1);
            return await LoadFromStorageAsync(partitionKey, rowKey, table, container, doNotLoadContent);
        }

public async Task SaveToStorageAsync(CloudTable table, CloudBlobContainer container)
{
    if (this.Content.Length >= MaxTableContentLength)
    {
        using (var blobStream = new MemoryStream(this.Content))
        {
            var blob = container.GetBlockBlobReference(CreateContentId());
            await blob.UploadFromStreamAsync(blobStream);
        }
        this.Content = null;
    }

    await table.ExecuteAsync(TableOperation.Insert(this));
    var entityCopy = new EventEntity()
    {
        PartitionKey = CreatePartitionKey(this.Created),
        RowKey = this.RowKey,
        Id = this.Id,
        Description = this.Description,
        Created = this.Created,
    };
    await table.ExecuteAsync(TableOperation.Insert(entityCopy));
}

        public async Task LoadContentAsync(CloudTable table, CloudBlobContainer container)
        {
            if (this.Content != null) return;

            if (this.PartitionKey.StartsWith(DateKeyPrefix))
            {
                var entity = await LoadFromStorageAsync(this.ContentId, table, container);
                this.Content = entity.Content;
            }
            else
            {
                using (var blobStream = new MemoryStream())
                {
                    var blob = container.GetBlockBlobReference(CreateContentId());
                    await blob.DownloadToStreamAsync(blobStream);
                    blobStream.Position = 0;
                    this.Content = blobStream.ToArray();
                }
            }
        }
    }
}
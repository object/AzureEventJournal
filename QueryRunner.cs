using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json.Linq;

namespace AzureEventJournal
{
    public class QueryRunner
    {
        private readonly IList<Storage> _storages;
        private readonly QuerySettings _querySettings;

        public QueryRunner(IList<Storage> storages, QuerySettings querySettings)
        {
            _storages = storages;
            _querySettings = querySettings;
        }

        public async Task<IDictionary<string, object>> GetSingleResultAsync()
        {
            var filter = _querySettings.CreateQueryFilters().Single();
            var storage = _storages.Single();
            var results = await GetResultsAsync(storage, filter);
            return results.Any()
                ? await ConvertResultAsync(storage, results.Single(), false)
                : new Dictionary<string, object>();
        }

        public async Task<IList<IDictionary<string, object>>> GetMultipleResultsAsync()
        {
            var filters = _querySettings.CreateQueryFilters();

            var tasks = new List<Task<List<EventEntity>>>();
            var taskStorage = new Dictionary<int, Storage>();
            foreach (var storage in _storages)
            {
                foreach (var filter in filters)
                {
                    var task = GetResultsAsync(storage, filter);
                    tasks.Add(task);
                    taskStorage.Add(task.Id, storage);
                }
            }
            await Task.WhenAll(tasks);

            return _storages.Count == 1
                ? await GetResultsFromSingleCollectionAsync(tasks, _storages.Single())
                : await GetResultsFromMultipleCollectionsAsync(tasks, taskStorage);
        }

        public async Task<IDictionary<string, object>> GetResultCountAsync()
        {
            var filters = _querySettings.CreateQueryFilters();

            var tasks = new List<Task<List<EventEntity>>>();
            var taskStorage = new Dictionary<int, Storage>();
            foreach (var storage in _storages)
            {
                foreach (var filter in filters)
                {
                    var task = GetResultsAsync(storage, filter);
                    tasks.Add(task);
                    taskStorage.Add(task.Id, storage);
                }
            }
            await Task.WhenAll(tasks);

            var rowCount = tasks.Select(x => x.Result.Count).Sum();
            return new Dictionary<string, object> { { "count", rowCount } };
        }

        private async Task<IList<IDictionary<string, object>>> GetResultsFromSingleCollectionAsync(
            IList<Task<List<EventEntity>>> tasks, Storage storage)
        {
            var results = new List<IDictionary<string, object>>();
            var rows = new List<EventEntity>();
            foreach (var task in tasks)
            {
                if (_querySettings.Order == Order.Ascending)
                {
                    task.Result.Reverse();
                    if (!_querySettings.TopCount.HasValue)
                    {
                        rows.AddRange(task.Result);
                    }
                    else if (rows.Count < _querySettings.TopCount.Value)
                    {
                        rows.AddRange(task.Result.GetRange(0, Math.Min(task.Result.Count, _querySettings.TopCount.Value - rows.Count)));
                    }
                }
                else
                {
                    rows.AddRange(task.Result);
                }
            }
            foreach (var row in rows)
            {
                results.Add(await ConvertResultAsync(storage, row, false));
            }

            return results;
        }


        private async Task<IList<IDictionary<string, object>>> GetResultsFromMultipleCollectionsAsync(
            IList<Task<List<EventEntity>>> tasks, IDictionary<int, Storage> taskStorage)
        {
            var results = new List<IDictionary<string, object>>();
            var rows = new List<EventEntity>();
            var rowStorage = new Dictionary<string, Storage>();
            foreach (var task in tasks)
            {
                foreach (var row in task.Result)
                {
                    rowStorage.Add(row.RowKey, taskStorage[task.Id]);
                }
                rows.AddRange(task.Result);
            }

            rows = (_querySettings.Order == Order.Ascending
                ? rows.OrderBy(x => x.Created)
                : rows.OrderByDescending(x => x.Created)).ToList();
            foreach (var row in rows)
            {
                results.Add(await ConvertResultAsync(rowStorage[row.RowKey], row, true));
            }

            return _querySettings.TopCount.HasValue ?
                results.Take(_querySettings.TopCount.Value).ToList() :
                results;
        }

        private async Task<List<EventEntity>> GetResultsAsync(Storage storage, string filter)
        {
            var query = new TableQuery<EventEntity>().Where(filter);
            TableContinuationToken token = null;
            var rows = new List<EventEntity>();
            var rowCount = 0;
            var resultCount = 0;
            do
            {
                var seg = await storage.Table.ExecuteQuerySegmentedAsync(query, token);
                token = seg.ContinuationToken;
                foreach (var row in seg)
                {
                    // Only stop after reaching TOP count if we retrieve data in an order they were originally stored (descending)
                    if ((_querySettings.OnlyCount || _querySettings.Order == Order.Descending) &&
                        _querySettings.TopCount.HasValue && resultCount >= _querySettings.TopCount)
                        break;

                    if (!(_querySettings.Mode is QueryByTime) ||
                        row.Created >= (_querySettings.Mode as QueryByTime).TimeRange.StartTime
                        && row.Created < (_querySettings.Mode as QueryByTime).TimeRange.EndTime)
                    {
                        if (rowCount >= _querySettings.SkipCount)
                        {
                            rows.Add(row);
                            ++resultCount;
                        }
                        ++rowCount;
                    }
                }
            } while (token != null && (!_querySettings.TopCount.HasValue
                                       || _querySettings.Order == Order.Ascending
                                       || resultCount < _querySettings.TopCount));

            return rows;
        }

        private async Task<IDictionary<string, object>> ConvertResultAsync(Storage storage, EventEntity row, bool includeCollectionName)
        {
            var kv = new Dictionary<string, object>();
            if (includeCollectionName) kv.Add("Collection", storage.Table.Name);
            if (row.Id != null) kv.Add("Id", row.Id);
            if (row.ProgramId != null) kv.Add("ProgramId", row.ProgramId);
            if (row.CarrierId != null) kv.Add("CarrierId", row.CarrierId);
            if (row.ServiceName != null) kv.Add("ServiceName", row.ServiceName);
            if (row.Description != null) kv.Add("Description", row.Description);
            kv.Add("Created", row.Created);
            kv.Add("IdPartitionKey", EventEntity.CreatePartitionKey(row.Id ?? row.ProgramId));
            kv.Add("DatePartitionKey", EventEntity.CreatePartitionKey(row.Created));
            kv.Add("RowKey", row.RowKey);
            if (!_querySettings.DoNotLoadContent)
            {
                await row.LoadContentAsync(storage.Table, storage.BlobContainer);

                if (row.Content != null)
                {
                    object contentData;
                    var contentText = Utils.Unzip(row.Content);
                    try
                    {
                        contentData = JObject.Parse(contentText);
                    }
                    catch
                    {
                        contentData = contentText;
                    }
                    kv.Add("Content", contentData);
                }
            }
            return kv;
        }
    }
}
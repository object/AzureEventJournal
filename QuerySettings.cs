using System;
using System.Linq;
using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureEventJournal
{
    public enum Order
    {
        Descending,
        Ascending
    }

    public class Storage
    {
        public readonly CloudTable Table;
        public readonly CloudBlobContainer BlobContainer;

        public Storage(CloudTable table, CloudBlobContainer blobContainer)
        {
            this.Table = table;
            this.BlobContainer = blobContainer;
        }
    }

    public class TimeRange
    {
        public readonly DateTime StartTime;
        public readonly DateTime EndTime;

        public TimeRange(DateTime startTime, DateTime endTime)
        {
            this.StartTime = startTime.ToUniversalTime();
            this.EndTime = endTime.ToUniversalTime();
        }

        public override bool Equals(object obj)
        {
            if (!(obj is TimeRange)) return false;
            var range = obj as TimeRange;
            return this.StartTime == range.StartTime && this.EndTime == range.EndTime;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(this.StartTime, this.EndTime);
        }

        public override string ToString()
        {
            return $"[{this.StartTime:u} .. {this.EndTime:u}]";
        }

        public IList<TimeRange> SplitIntoDates(Order order)
        {
            var dates = new List<TimeRange>();

            if (this.StartTime.Date == this.EndTime.Date)
            {
                dates.Add(this);
            }
            else if (order == Order.Ascending)
            {
                dates.Add(new TimeRange(this.StartTime, this.StartTime.Date.AddDays(1)));
                for (var time = this.StartTime.Date.AddDays(1); time < this.EndTime.Date; time = time.AddDays(1))
                {
                    dates.Add(new TimeRange(time, time.AddDays(1)));
                }
                if (this.EndTime > this.EndTime.Date)
                    dates.Add(new TimeRange(this.EndTime.Date, this.EndTime));
            }
            else
            {
                if (this.EndTime > this.EndTime.Date)
                    dates.Add(new TimeRange(this.EndTime.Date, this.EndTime));
                for (var time = this.EndTime.Date.AddDays(-1); time > this.StartTime.Date; time = time.AddDays(-1))
                {
                    dates.Add(new TimeRange(time, time.AddDays(1)));
                }
                dates.Add(new TimeRange(this.StartTime, this.StartTime.Date.AddDays(1)));
            }
            return dates;
        }
    }

    public interface IQueryMode { }

    public class QueryById : IQueryMode
    {
        public readonly string Id;
        public QueryById(string id) { this.Id = id; }
    }

    public class QueryByTime : IQueryMode
    {
        public readonly TimeRange TimeRange;
        public QueryByTime(TimeRange timeRange) { this.TimeRange = timeRange; }
    }

    public class GetByKey : IQueryMode
    {
        public readonly string PartitionKey;
        public readonly string RowKey;
        public GetByKey(string partitionKey, string rowKey) { this.PartitionKey = partitionKey; this.RowKey = rowKey; }
    }

    public class QueryParams
    {
        public int? TopCount { get; set; }
        public int SkipCount { get; set; }
        public Order Order { get; set; }
        public bool DoNotLoadContent { get; set; }
        public bool OnlyCount { get; set; }
        public bool DisableBuckets { get; set; }

        public QueryParams(HttpRequest req)
        {
            var paramValue = req.Query["top"];
            if (!string.IsNullOrEmpty(paramValue)) this.TopCount = int.Parse(paramValue);

            paramValue = req.Query["skip"];
            if (!string.IsNullOrEmpty(paramValue)) this.SkipCount = int.Parse(paramValue);

            paramValue = req.Query["order"];
            if (!string.IsNullOrEmpty(paramValue))
                this.Order =
                    string.Equals(paramValue, "asc", StringComparison.CurrentCultureIgnoreCase) ||
                    string.Equals(paramValue, "ascending", StringComparison.CurrentCultureIgnoreCase)
                        ? Order.Ascending
                        : Order.Descending;

            paramValue = req.Query["noContent"];
            if (!string.IsNullOrEmpty(paramValue)) this.DoNotLoadContent = bool.Parse(paramValue);

            paramValue = req.Query["onlyCount"];
            if (!string.IsNullOrEmpty(paramValue)) this.OnlyCount = bool.Parse(paramValue);

            paramValue = req.Query["noBuckets"];
            if (!string.IsNullOrEmpty(paramValue)) this.DisableBuckets = bool.Parse(paramValue);
        }
    }

    public class QuerySettings
    {
        public IQueryMode Mode { get; set; }
        public int? TopCount { get; set; }
        public int SkipCount { get; set; }
        public Order Order { get; set; }
        public bool DoNotLoadContent { get; set; }
        public bool OnlyCount { get; set; }
        public bool UseBuckets => this.Mode is QueryByTime && !this.TopCount.HasValue && this.SkipCount == 0;
        public bool DisableBuckets { get; set; }

        public QuerySettings(string id, QueryParams queryParams = null)
            : this(new QueryById(id), queryParams)
        {
        }

        public QuerySettings(DateTime fromTime, DateTime toTime, QueryParams queryParams = null)
            : this(new QueryByTime(new TimeRange(fromTime, toTime)), queryParams)
        {
        }

        public QuerySettings(string partitionKey, string rowKey, QueryParams queryParams = null)
            : this(new GetByKey(partitionKey, rowKey), queryParams)
        {
        }

        private QuerySettings(IQueryMode queryMode, QueryParams queryParams)
        {
            this.Mode = queryMode;
            if (queryParams != null)
            {
                TopCount = queryParams.TopCount;
                SkipCount = queryParams.SkipCount;
                Order = queryParams.Order;
                DoNotLoadContent = queryParams.DoNotLoadContent;
                OnlyCount = queryParams.OnlyCount;
                DisableBuckets = queryParams.DisableBuckets;
            }
        }

        public IList<string> CreateQueryFilters()
        {
            if (this.Mode is QueryById)
            {
                return new[] { EventEntity.CreateFilterForId((this.Mode as QueryById).Id) };
            }
            else if (this.Mode is QueryByTime)
            {
                var timeRange = (this.Mode as QueryByTime).TimeRange;
                if (this.UseBuckets && !this.DisableBuckets)
                {
                    var buckets = timeRange.SplitIntoDates(this.Order);
                    return buckets.Select(x => EventEntity.CreateFilterForTimeInterval(x.StartTime, x.EndTime)).ToList();
                }
                else
                {
                    return new[]
                    {
                        EventEntity.CreateFilterForTimeInterval(
                            timeRange.StartTime,
                            timeRange.EndTime)
                    };
                }
            }
            if (this.Mode is GetByKey)
            {
                var keys = this.Mode as GetByKey;
                return new[] { EventEntity.CreateFilterForKey(keys.PartitionKey, keys.RowKey) };
            }
            else
            {
                return new string[] { };
            }
        }
    }
}
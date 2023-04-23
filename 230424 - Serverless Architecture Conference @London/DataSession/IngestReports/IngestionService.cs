using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using IngestReports.Models;
using Newtonsoft.Json;
using Parquet;

namespace IngestReports
{
    internal class IngestionService
    {
        private string _executionEnvironment = "[prefix]";
        private string _storageConnection = "[connStr]";
        private string _dbConnection = "[database]";

        internal void Run()
        {
            var container = new BlobContainerClient(_storageConnection, "reports");
            var sourcing=new SourcingService();
            var reports=sourcing.Reports(new DateTime(2021,1,1));

            foreach (var item in reports)
            {
                var (currentDate, conversions, clicks, impressions) = item;

                //Step 0 - SQL
                //RelationalDatabase(conversions, clicks, impressions);

                //Step 1 - anonymous batches for landing
                LandingZone(container, conversions, clicks, impressions);

                //Step 2 - partitioned batches
                PartitionedDataLake(container, currentDate, conversions, clicks, impressions);

                //Step 3 - JSON lines
                PartitionedDataLakeAppendMode(container, currentDate, conversions, clicks, impressions);

                //Step 4 - Parquet
                ParquetDataLake(container, currentDate, conversions, clicks, impressions);
            }
        }


        private void ParquetDataLake(BlobContainerClient container, DateTime currentDate, ConversionEvent[]? conversions, ClickEvent[]? clicks, ImpressionEvent[]? impressions)
        {
            UploadBatchParquet(conversions, "Conversions", currentDate, container);
            UploadBatchParquet(clicks, "Clicks", currentDate, container);
            UploadBatchParquet(impressions.SelectMany(p => ImpressionEventFlatten.From(p)), "Impressions", currentDate, container);
        }

        private void PartitionedDataLakeAppendMode(BlobContainerClient container, DateTime currentDate, ConversionEvent[]? conversions, ClickEvent[]? clicks, ImpressionEvent[]? impressions)
        {
            UploadBatchPartitionedAppend(conversions, "Conversions", currentDate, container);
            UploadBatchPartitionedAppend(clicks, "Clicks", currentDate, container);
            UploadBatchPartitionedAppend(impressions, "Impressions", currentDate, container);
        }

        private void PartitionedDataLake(BlobContainerClient container, DateTime currentDate, ConversionEvent[]? conversions, ClickEvent[]? clicks, ImpressionEvent[]? impressions)
        {
            UploadBatchPartitioned(conversions, "Conversions", currentDate, container);
            UploadBatchPartitioned(clicks, "Clicks", currentDate, container);
            UploadBatchPartitioned(impressions, "Impressions", currentDate, container);
        }

        private void LandingZone(BlobContainerClient container, ConversionEvent[]? conversions, ClickEvent[]? clicks, ImpressionEvent[]? impressions)
        {
            UploadBatch(conversions, clicks, impressions, container);
        }

        private void RelationalDatabase(ConversionEvent[]? conversions, ClickEvent[]? clicks, ImpressionEvent[]? impressions)
        {
            WriteToDatabase("advertising.Conversions", conversions);
            WriteToDatabase("advertising.Clicks", clicks);
            WriteToDatabase("advertising.Impressions", impressions.SelectMany(p => ImpressionEventFlatten.From(p)));
        }

        private void WriteToDatabase<T>(string name, IEnumerable<T> data) where T : class, new()
        {
            var tbl = Utils.AsTable<T>();
            tbl.TableName = name;
            var rows = data.Select(p => tbl.NewRow().FillWith(p));
            foreach (var row in rows)
            {
                tbl.Rows.Add(row);
            }
            tbl.AutoSqlBulkCopy(_dbConnection);
        }
        private void UploadBatchPartitioned(object obj, string type, DateTime date, BlobContainerClient container)
        {
            var blob = container.GetBlobClient(
                $"{_executionEnvironment}/partitioned/" +
                $"Type={type}/Year={date.Year}/Month={date.Month.ToString().PadLeft(2, '0')}/Day={date.Day.ToString().PadLeft(2, '0')}/report.json");
            BlobUpload(blob, obj);
        }

        private void UploadBatchParquet<T>(IEnumerable<T> obj, string type, DateTime date, BlobContainerClient container) where T : class, new()
        {
            var blob = container.GetBlobClient(
                $"{_executionEnvironment}/partitioned/" +
                $"Type={type}/Year={date.Year}/Month={date.Month.ToString().PadLeft(2, '0')}/Day={date.Day.ToString().PadLeft(2, '0')}/report.parquet");
            BlobUploadParquet(blob, obj);
        }

        private void UploadBatchPartitionedAppend(object obj, string type, DateTime date, BlobContainerClient container)
        {
            var blob = container.GetAppendBlobClient(
                $"{_executionEnvironment}/append/" +
                $"Type={type}/Year={date.Year}/report.json");
            BlobAppend(blob, obj);
        }

        private void UploadBatch(object a, object b, object c, BlobContainerClient container)
        {
            UploadBatch(a, container);
            UploadBatch(b, container);
            UploadBatch(c, container);
        }

        private void BlobUploadParquet<T>(BlobClient blob, IEnumerable<T> obj) where T : class, new()
        {
            using (var memory = new MemoryStream())
            {
                ParquetConvert.Serialize(obj, memory);
                memory.Position = 0;
                blob.Upload(memory);
            }
        }
        private void BlobUpload(BlobClient blob, object obj)
        {
            var json = JsonConvert.SerializeObject(obj);
            using (var memory = new MemoryStream())
            using (var streamWriter = new StreamWriter(memory))
            {
                streamWriter.Write(json);
                memory.Position = 0;
                blob.Upload(memory);
            }
        }

        private void BlobAppend(AppendBlobClient blob, object obj)
        {
            blob.CreateIfNotExists();
            var json = JsonConvert.SerializeObject(obj);
            using (var memory = new MemoryStream())
            using (var streamWriter = new StreamWriter(memory))
            {
                streamWriter.Write(json);
                memory.Position = 0;
                blob.AppendBlock(memory);
            }
        }
        private void UploadBatch(object obj, BlobContainerClient container)
        {
            var blob = container.GetBlobClient(
                $"{_executionEnvironment}/batch/{Guid.NewGuid()}.json");
            BlobUpload(blob, obj);
        }
    }
}

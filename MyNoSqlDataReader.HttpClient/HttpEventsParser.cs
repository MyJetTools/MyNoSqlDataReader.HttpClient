using System.Text;
using MyNoSqlDataReader.Core.Db;
using MyNoSqlDataReader.Core.SyncEvents;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlDataReader.HttpClient;

public class HttpEventsParser<TDbRow> : IInitTableSyncEvents<TDbRow>  where TDbRow : IMyNoSqlEntity, new()
{
    public InitTableSyncEvent<TDbRow> ParseInitTable(SyncContract syncContract)
    {
        var json = Encoding.UTF8.GetString(syncContract.Payload.Span);

        var contract = Newtonsoft.Json.JsonConvert.DeserializeObject<List<TDbRow>>(json);

        var partitions = new SortedDictionary<String, DbPartition<TDbRow>>();

        foreach (var group in contract.GroupBy(dbRow => dbRow.PartitionKey))
        {
            var dbPartition = new DbPartition<TDbRow>(group.Key);
            dbPartition.BulkInsertOrReplace(group);
            partitions.Add(group.Key, dbPartition);
        }

        return new InitTableSyncEvent<TDbRow>(partitions);
    }

    public InitPartitionSyncEvent<TDbRow> ParseInitPartitions(SyncContract syncContract)
    {
        var json = Encoding.UTF8.GetString(syncContract.Payload.Span);
        var contract = Newtonsoft.Json.JsonConvert.DeserializeObject<Dictionary<string, List<TDbRow>>>(json);
        var updatedPartitions = new Dictionary<string, DbPartition<TDbRow>>();

        foreach (var (partitionKey, dbRows) in contract)
        {
            var dbPartition = new DbPartition<TDbRow>(partitionKey);
            dbPartition.BulkInsertOrReplace(dbRows);

            updatedPartitions.Add(partitionKey, dbPartition);
        }

        return new InitPartitionSyncEvent<TDbRow>(updatedPartitions);
    }

    public UpdateRowsSyncEvent<TDbRow> ParseUpdateRows(SyncContract syncContract)
    {
        var json = Encoding.UTF8.GetString(syncContract.Payload.Span);
        var changedRows = Newtonsoft.Json.JsonConvert.DeserializeObject<List<TDbRow>>(json);
        return new UpdateRowsSyncEvent<TDbRow>(changedRows);
    }

    public DeleteRowsSyncEvent ParseDeleteRows(SyncContract syncContract)
    {
        var json = Encoding.UTF8.GetString(syncContract.Payload.Span);
        var deletedRows = Newtonsoft.Json.JsonConvert.DeserializeObject<Dictionary<string, List<string>>>(json);
        return new DeleteRowsSyncEvent(deletedRows);
    }
}
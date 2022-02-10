using System.Text;
using MyNoSqlDataReader.Core.SyncEvents;

namespace MyNoSqlDataReader.HttpClient;

public static class HttpContracts
{
    private static SyncContract? Parse(ref int pos, byte[] payload)
    {
        var header = ParsePascalString(ref pos, payload);
        
        if (header == null)
        {
            return null;
        }

        var (syncEventType, tableName) = ParseHeaderString(header);

        var resultPayload = ParseByteArray(ref pos, payload);

        return new SyncContract(tableName, syncEventType, resultPayload);
    }
    
    public static IEnumerable<SyncContract> Read(byte[] payload)
    {
        var position = 0;

        var syncContract = Parse(ref position, payload);
        while (syncContract != null)
        {
            yield return syncContract;
            syncContract = Parse(ref position, payload);
        }
    }
    
    
    private static SyncEventType ParseSyncEventType(string src)
    {

        if (src == "initTable")
            return SyncEventType.InitTable;
        
        if (src == "initPartitions")
            return SyncEventType.InitPartitions;
        
        if (src == "updateRows")
            return SyncEventType.UpdateRows;
        
        if (src == "deleteRows")
            return SyncEventType.DeleteRows;

        throw new Exception("Invalid Header Change Event Type");

    }


    private static string? ParsePascalString(ref int pos, byte[] payload)
    {
        if (pos >= payload.Length)
            return null;
        
        var len = payload[pos];
        pos++;

        var result = Encoding.UTF8.GetString(payload.AsSpan(pos, len));
        pos += len;

        return result;
    }

    private static int ParseInt(ref int pos, byte[] payload)
    {
        var result = BitConverter.ToInt32(payload.AsSpan(pos, 4));
        pos += 4;

        return result;
    }

    private static ReadOnlyMemory<byte> ParseByteArray(ref int pos, byte[] payload)
    {
        var len = ParseInt(ref pos, payload);

        var result = new ReadOnlyMemory<byte>(payload, pos,  len);
        pos += pos + len;

        return result;
    }


    private static (SyncEventType eventType, string tableName) ParseHeaderString(string headerString)
    {
        var index = headerString.IndexOf('=');
        
        if (index<0)
        {
            throw new Exception($"Invalid header string {headerString}" );
        }

        var tableName = headerString.Substring(index + 1);

        var eventName = headerString.Substring(0, index);

        var eventType = ParseSyncEventType(eventName);

        return (eventType, tableName);
    } 
}
using Flurl;
using Flurl.Http;
using MyNoSqlDataReader.Core;
using MyNoSqlDataReader.Core.SyncEvents;

namespace MyNoSqlDataReader.HttpClient;

using MyNoSqlServer.Abstractions;

public class MyNoSqlHttpConnection
{
    public enum LogLevel{
        Error, Info
    }

    public struct LogItem
    {
        public DateTime DateTime { get; private set; }
        public LogLevel Level { get; private set; }
        public string Process { get; private set; }
        public string Message { get; private set; }
        public Exception? Exception { get; private set; }
        
        public static LogItem Create(LogLevel logLevel, string process, string message, Exception? exception)
        {
            return new LogItem
            {
                DateTime = DateTime.UtcNow,
                Level = logLevel,
                Process = process,
                Message = message,
                Exception = exception
            };
        }
    }
    
    public class NewSessionModel
    {
        public string? Session { get; set; }
    }

    private readonly Func<string> _getHost;
    private string _appName;
    private string _libVersion;

    private readonly Dictionary<string, IMyNoSqlDataReader> _subscribers = new();
    
    public TimeSpan ConnectTimeout = TimeSpan.FromSeconds(3);
    
    public MyNoSqlHttpConnection(Func<string> getHost, string appName, string libVersion)
    {
        _getHost = getHost;
        _appName = appName;
        _libVersion = libVersion;
    }



    private readonly object _lockObject = new();
    private Task? _connectLoopTask;

    private string? _sessionId;

    private Action<LogItem>? _logCallback;

    private void PlugLog(Action<LogItem> logCallback)
    {
        _logCallback = logCallback;
    }


    private void WriteLogToConsole(ref LogItem item)
    {
        Console.WriteLine("==== Socket Log Record =====");
        Console.WriteLine($"DateTime: {item.DateTime:s}");
        Console.WriteLine($"Level: {item.Level}");
        Console.WriteLine($"Process: {item.Process}");
        Console.WriteLine($"Message: {item.Message}");

        if (item.Exception != null)
        {
            Console.WriteLine($"Exception: {item.Exception}");
        }
    }


    private void WriteLog(LogItem item)
    {
        if (_logCallback == null)
        {
            WriteLogToConsole(ref item);
        }
        else
        {
            if (item.Level == LogLevel.Error)
            {
                WriteLogToConsole(ref item);
            }

            _logCallback(item);
        }
    }

    private async Task ConnectLoopAsync()
    {

        while (true)
        {
            try
            {
                if (_sessionId == null)
                    await GetSessionAsync();

                await SubscribeToTablesAsync();

                await GetChangesLoopAsync();

            }
            catch (Exception e)
            {
                WriteLog(LogItem.Create(LogLevel.Error, "ConnectLoop", e.Message, e));
                await Task.Delay(ConnectTimeout);
            }
            
        }
        
    }

    public MyNoSqlDataReader<TDbRow> Subscribe<TDbRow>(string tableName) where TDbRow: IMyNoSqlEntity, new()
    {
        var parser = new HttpEventsParser<TDbRow>();
        var result = new MyNoSqlDataReader<TDbRow>(tableName, parser);
        _subscribers.Add(tableName, result);
        return result;
    }


    private async Task GetChangesLoopAsync()
    {
        try
        {
            while (true)
            {
                await GetChangesAsync();
            }
        }
        finally
        {
            WriteLog(LogItem.Create(LogLevel.Info, "Session is disconnected", $"Id={_sessionId}", null));
            _sessionId = null;
        }

    }

    private async Task SubscribeToTablesAsync()
    {
        foreach (var tableName in _subscribers.Keys)
        {
            await _getHost().AppendPathSegment("DataReader")
                .AppendPathSegment("Subscribe")    
                .WithHeader("session", _sessionId)
                .SetQueryParam("tableName", tableName)
                .PostAsync();
        }
    }

    private async Task GetChangesAsync()
    {
        var syncPayload = await _getHost().AppendPathSegment("DataReader")
            .AppendPathSegment("GetChanges")
            .WithHeader("session", _sessionId)
            .PostAsync().ReceiveBytes();


        foreach (var syncContract in HttpContracts.Read(syncPayload))
        {
            if (_subscribers.TryGetValue(syncContract.TableName, out var dataReader))
            {
                dataReader.UpdateData(syncContract);
            }
        }

    }

    private async Task GetSessionAsync()
    {
        var result = await _getHost().AppendPathSegment("DataReader")
            .AppendPathSegment("Greeting")
            .SetQueryParam("name", "Test")
            .SetQueryParam("version", "2.3.4")
            .PostAsync().ReceiveJson<NewSessionModel>();

        _sessionId =  result.Session;
        
        WriteLog(LogItem.Create(LogLevel.Info, "New Session", $"Id={_sessionId}", null));
    }

    public void Start()
    {

        lock (_lockObject)
        {

            if (_subscribers.Count == 0)
            {
                throw new Exception("There are no subscribers to start MyNoSqlDataReader");
            }
            
            if (_connectLoopTask != null)
                throw new Exception("You can not start read loop two times");

            _connectLoopTask = ConnectLoopAsync();
        }
    }
    
}
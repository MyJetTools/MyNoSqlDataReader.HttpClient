using Flurl;
using Flurl.Http;
using MyNoSqlDataReader.Core;
using MyNoSqlDataReader.Core.SyncEvents;
namespace MyNoSqlDataReader.HttpClient;

public class MyNoSqlHttpConnection : MyNoSqlDataReaderConnection
{
    public class NewSessionModel
    {
        public string? Session { get; set; }
    }

    private readonly Func<string> _getHost;
    private readonly string _appName;

    public TimeSpan ConnectTimeout = TimeSpan.FromSeconds(3);
    
    private static readonly Lazy<string> ReaderVersion = new (() =>
    {
        try
        {
            return typeof(MyNoSqlHttpConnection).Assembly.GetName().Version?.ToString() ?? "Unknown";
        }
        catch (Exception)
        {
            return "Unknown";
        }
    });
    
    public MyNoSqlHttpConnection(Func<string> getHost, string appName)
    {
        _getHost = getHost;
        _appName = appName;
    }

    private readonly object _lockObject = new();
    private Task? _connectLoopTask;

    private string? _sessionId;

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
                Logger.Write(Logger.LogItem.Create(Logger.LogLevel.Error, "ConnectLoop", e.Message, e));
                await Task.Delay(ConnectTimeout);
            }
            
        }
        
    }

    protected override IInitTableSyncEvents<TDbRow> CreateInitTableSyncEvents<TDbRow>()
    {
        return new HttpEventsParser<TDbRow>();
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
            Logger.Write(Logger.LogItem.Create(Logger.LogLevel.Info, "Session is disconnected", $"Id={_sessionId}", null));
            _sessionId = null;
        }

    }

    private async Task SubscribeToTablesAsync()
    {
        foreach (var tableName in SubscribedTables)
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
            HandleIncomingPacket(syncContract);
        }

    }

    private async Task GetSessionAsync()
    {
        var result = await _getHost().AppendPathSegment("DataReader")
            .AppendPathSegment("Greeting")
            .SetQueryParam("name", _appName)
            .SetQueryParam("version", ReaderVersion.Value)
            .PostAsync().ReceiveJson<NewSessionModel>();

        _sessionId =  result.Session;
        
        Logger.Write(Logger.LogItem.Create(Logger.LogLevel.Info, "New Session", $"Id={_sessionId}", null));
    }

    public void Start()
    {

        lock (_lockObject)
        {

            if (SubscribersCount == 0)
            {
                throw new Exception("There are no subscribers to start MyNoSqlDataReader");
            }
            
            if (_connectLoopTask != null)
                throw new Exception("You can not start read loop two times");

            _connectLoopTask = ConnectLoopAsync();
        }
    }
    
}
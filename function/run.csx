#r "Newtonsoft.Json"
using System.Net.Http;
using Newtonsoft.Json;
using System.Text;

class JobBody
{
    public int job_id {get; set;}
    public NotebookParams notebook_params {get; set;}
}

class NotebookParams
{
    public string fileName {get; set;}
    public string storageKey {get; set;}
    public string storageName {get; set;}
}


public static void Run(Stream myBlob, string name, ILogger log)
{
    log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");
    RunDatabricksJob(log, name);
}

public static async Task RunDatabricksJob(ILogger log, string fileName)
{
    var client = new HttpClient();
    string token = Environment.GetEnvironmentVariable("DatabricksToken");
    string url = Environment.GetEnvironmentVariable("DatabricksURL") + "/api/2.0/jobs/run-now";
    var body = new JobBody();
    string jobIdString = Environment.GetEnvironmentVariable("DatabricksJobID");
    string storageKey = Environment.GetEnvironmentVariable("StorageKey");
    string storageName = Environment.GetEnvironmentVariable("StorageName");

    body.job_id = Int32.Parse(jobIdString);
    var notebookParams = new NotebookParams();
    notebookParams.fileName = fileName;
    notebookParams.storageName = storageName;
    notebookParams.storageKey = storageKey;
    body.notebook_params = notebookParams;

    log.LogInformation(JsonConvert.SerializeObject(body));


    client.DefaultRequestHeaders.Add("Authorization","Bearer " + token);
    var response = await client.PostAsync(url, body.AsJson());
    var contents = await response.Content.ReadAsStringAsync();
    log.LogInformation(contents);

}

public static StringContent AsJson(this object o)
  => new StringContent(JsonConvert.SerializeObject(o), Encoding.UTF8, "application/json");

#load "../shared/chunk.csx"
#load "../shared/sendDownstream.csx"
#load "../shared/getEnvironmentVariable.csx"

#r "Microsoft.WindowsAzure.Storage"

using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host.Bindings.Runtime;
using Microsoft.WindowsAzure.Storage.Blob;

public static async Task Run(Chunk inputChunk, Binder binder, Binder logTransmissions, TraceWriter log)
{
    log.Info($"C# NSG Flow Logs Logstash (Queue trigger) function processed: {inputChunk}");

    string nsgSourceDataAccount = getEnvironmentVariable("nsgSourceDataAccount");
    if (nsgSourceDataAccount.Length == 0)
    {
        log.Error("Value for nsgSourceDataAccount is required.");
        throw new ArgumentNullException("nsgSourceDataAccount", "Please supply in this setting the name of the connection string from which NSG logs should be read.");
    }

    var attributes = new Attribute[]
    {    
        new BlobAttribute(inputChunk.BlobName),
        new StorageAccountAttribute(nsgSourceDataAccount)
    };

    string nsgMessagesString;
    try
    {
        byte[] nsgMessages = new byte[inputChunk.Length];
        CloudBlockBlob blob = await binder.BindAsync<CloudBlockBlob>(attributes);
        await blob.DownloadRangeToByteArrayAsync(nsgMessages, 0, inputChunk.Start, inputChunk.Length);
        nsgMessagesString = System.Text.Encoding.UTF8.GetString(nsgMessages);
    }
    catch (Exception ex)
    {
        log.Error(string.Format("Error binding blob input: {0}", ex.Message));
        throw ex;
    }

    // skip past the leading comma
    string trimmedMessages = nsgMessagesString.Trim();
    int curlyBrace = trimmedMessages.IndexOf('{');
    string newClientContent = "{\"records\":[";
    newClientContent += trimmedMessages.Substring(curlyBrace);
    newClientContent += "]}";

    var attributesLog = new Attribute[]
    {
        new BlobAttribute("transmissions/" + inputChunk.LastBlockName + ".json"),
        new StorageAccountAttribute("AzureWebJobsStorage")
    };

    try
    {
        TextWriter writer = await logTransmissions.BindAsync<TextWriter>(attributesLog);
        await writer.WriteAsync(newClientContent);
    } catch (Exception ex)
    {
        log.Error(string.Format("Error binding blob output: {0}", ex.Message));
        throw ex;
    }

    await SendMessagesDownstream(newClientContent, log);
}


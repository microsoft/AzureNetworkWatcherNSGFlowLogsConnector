using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Threading.Tasks;

namespace NwNsgProject
{
    public static class Stage3QueueTrigger
    {
        [FunctionName("Stage3QueueTrigger")]
        public static async Task Run(
            [QueueTrigger("stage2", Connection = "AzureWebJobsStorage")]Chunk inputChunk,
            Binder binder, 
            Binder cefLogBinder,
            Binder errorRecordBinder,
            TraceWriter log)
        {
//            log.Info($"C# Queue trigger function processed: {inputChunk}");

            string nsgSourceDataAccount = Util.GetEnvironmentVariable("nsgSourceDataAccount");
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

            await SendMessagesDownstream(newClientContent, log);

            string logOutgoingCEF = Util.GetEnvironmentVariable("logOutgoingCEF");
            Boolean flag;
            if (Boolean.TryParse(logOutgoingCEF, out flag))
            {
                if (flag)
                {
                    await CEFLog(newClientContent, cefLogBinder, errorRecordBinder, log);
                }
            }
        }

        public static async Task SendMessagesDownstream(string myMessages, TraceWriter log)
        {
            //
            // myMessages looks like this:
            // {
            //   "records":[
            //     {...},
            //     {...}
            //     ...
            //   ]
            // }
            //
            string outputBinding = Util.GetEnvironmentVariable("outputBinding");
            if (outputBinding.Length == 0)
            {
                log.Error("Value for outputBinding is required. Permitted values are: 'logstash', 'arcsight'.");
                return;
            }

            switch (outputBinding)
            {
                case "logstash":
                    await Util.obLogstash(myMessages, log);
                    break;
                case "arcsight":
                    await Util.obArcsight(myMessages, log);
                    break;
                case "splunk":
                    await Util.obSplunk(myMessages, log);
                    break;
                case "eventhub":
                    await Util.obEventHub(myMessages, log);
                    break;
            }
        }

        static async Task CEFLog(string newClientContent, Binder cefLogBinder, Binder errorRecordBinder, TraceWriter log)
        {
            int count = 0;
            Byte[] transmission = new Byte[] { };

            foreach (var message in Util.convertToCEF(newClientContent, errorRecordBinder, log))
            {

                try
                {
                    transmission = Util.AppendToTransmission(transmission, message);

                    // batch up the messages
                    if (count++ == 1000)
                    {
                        Guid guid = Guid.NewGuid();
                        var attributes = new Attribute[]
                        {
                            new BlobAttribute(String.Format("ceflog/{0}", guid)),
                            new StorageAccountAttribute("cefLogAccount")
                        };

                        CloudBlockBlob blob = await cefLogBinder.BindAsync<CloudBlockBlob>(attributes);
                        await blob.UploadFromByteArrayAsync(transmission, 0, transmission.Length);

                        count = 0;
                        transmission = new Byte[] { };
                    }
                }
                catch (Exception ex)
                {
                    log.Error($"Exception logging CEF output: {ex.Message}");
                }
            }

            if (count != 0)
            {
                Guid guid = Guid.NewGuid();
                var attributes = new Attribute[]
                {
                    new BlobAttribute(String.Format("ceflog/{0}", guid)),
                    new StorageAccountAttribute("cefLogAccount")
                };

                CloudBlockBlob blob = await cefLogBinder.BindAsync<CloudBlockBlob>(attributes);
                await blob.UploadFromByteArrayAsync(transmission, 0, transmission.Length);
            }
        }

    }
}

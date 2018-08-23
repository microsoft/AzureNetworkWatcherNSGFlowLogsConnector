using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Net.Sockets;
using Newtonsoft.Json;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography.X509Certificates;
using System.Net.Security;

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
                    await obLogstash(myMessages, log);
                    break;
                case "arcsight":
                    await obArcsight(myMessages, log);
                    break;
                case "splunk":
                    await obSplunk(myMessages, log);
                    break;
            }
        }

        static async Task CEFLog(string newClientContent, Binder cefLogBinder, Binder errorRecordBinder, TraceWriter log)
        {
            int count = 0;
            Byte[] transmission = new Byte[] { };

            foreach (var message in convertToCEF(newClientContent, errorRecordBinder, log))
            {

                try
                {
                    transmission = AppendToTransmission(transmission, message);

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

        static System.Collections.Generic.IEnumerable<string> convertToCEF(string newClientContent, Binder errorRecordBinder, TraceWriter log)
        {
            // newClientContent is a json string with records

            NSGFlowLogRecords logs = JsonConvert.DeserializeObject<NSGFlowLogRecords>(newClientContent);

            string logIncomingJSON = Util.GetEnvironmentVariable("logIncomingJSON");
            Boolean flag;
            if (Boolean.TryParse(logIncomingJSON, out flag))
            {
                if (flag)
                {
                    logErrorRecord(newClientContent, errorRecordBinder, log).Wait();
                }
            } 

            string cefRecordBase = "";
            foreach (var record in logs.records)
            {
                float version = record.properties.Version;

                cefRecordBase = record.MakeCEFTime();
                cefRecordBase += "|Microsoft.Network";
                cefRecordBase += "|NETWORKSECURITYGROUPS";
                cefRecordBase += "|" + version.ToString("0.0");
                cefRecordBase += "|" + record.category;
                cefRecordBase += "|" + record.operationName;
                cefRecordBase += "|1";  // severity is always 1
                cefRecordBase += "|deviceExternalId=" + record.MakeDeviceExternalID();

                foreach (var outerFlows in record.properties.flows)
                {
                    // expectation is that there is only ever 1 item in record.properties.flows
                    string cefOuterFlowRecord = cefRecordBase;
                    cefOuterFlowRecord += String.Format(" cs1={0}", outerFlows.rule);
                    cefOuterFlowRecord += String.Format(" cs1Label=NSGRuleName");

                    foreach (var innerFlows in outerFlows.flows)
                    {
                        var cefInnerFlowRecord = cefOuterFlowRecord;
                        
                        var firstFlowTupleEncountered = true;
                        foreach (var flowTuple in innerFlows.flowTuples)
                        {
                            var tuple = new NSGFlowLogTuple(flowTuple, version);

                            if (firstFlowTupleEncountered)
                            {
                                cefInnerFlowRecord += (tuple.GetDirection == "I" ? " dmac=" : " smac=") + innerFlows.MakeMAC();
                                firstFlowTupleEncountered = false;
                            }

                            yield return cefInnerFlowRecord + " " + tuple.ToString();
                        }
                    }
                }
            }
        }

        static async Task logErrorRecord(NSGFlowLogRecord errorRecord, Binder errorRecordBinder, TraceWriter log)
        {
            if (errorRecordBinder == null) { return; }

            Byte[] transmission = new Byte[] { };

            try
            {
                transmission = AppendToTransmission(transmission, errorRecord.ToString());

                Guid guid = Guid.NewGuid();
                var attributes = new Attribute[]
                {
                    new BlobAttribute(String.Format("errorrecord/{0}", guid)),
                    new StorageAccountAttribute("cefLogAccount")
                };

                CloudBlockBlob blob = await errorRecordBinder.BindAsync<CloudBlockBlob>(attributes);
                blob.UploadFromByteArray(transmission, 0, transmission.Length);

                transmission = new Byte[] { };
            }
            catch (Exception ex)
            {
                log.Error($"Exception logging record: {ex.Message}");
            }
        }

        static async Task logErrorRecord(string errorRecord, Binder errorRecordBinder, TraceWriter log)
        {
            if (errorRecordBinder == null) { return; }

            Byte[] transmission = new Byte[] { };

            try
            {
                transmission = AppendToTransmission(transmission, errorRecord);

                Guid guid = Guid.NewGuid();
                var attributes = new Attribute[]
                {
                    new BlobAttribute(String.Format("errorrecord/{0}", guid)),
                    new StorageAccountAttribute("cefLogAccount")
                };

                CloudBlockBlob blob = await errorRecordBinder.BindAsync<CloudBlockBlob>(attributes);
                blob.UploadFromByteArray(transmission, 0, transmission.Length);

                transmission = new Byte[] { };
            }
            catch (Exception ex)
            {
                log.Error($"Exception logging record: {ex.Message}");
            }
        }

        static async Task obArcsight(string newClientContent, TraceWriter log)
        {
            //
            // newClientContent looks like this:
            //
            // {
            //   "records":[
            //     {...},
            //     {...}
            //     ...
            //   ]
            // }
            //
            string arcsightAddress = Util.GetEnvironmentVariable("arcsightAddress");
            string arcsightPort = Util.GetEnvironmentVariable("arcsightPort");

            if (arcsightAddress.Length == 0 || arcsightPort.Length == 0)
            {
                log.Error("Values for arcsightAddress and arcsightPort are required.");
                return;
            }

            TcpClient client = new TcpClient(arcsightAddress, Convert.ToInt32(arcsightPort));
            NetworkStream stream = client.GetStream();

            int count = 0;
            Byte[] transmission = new Byte[] { };
            foreach (var message in convertToCEF(newClientContent, null, log))
            {

                try
                {
                    transmission = AppendToTransmission(transmission, message);

                    // batch up the messages
                    if (count++ == 1000)
                    {
                        await stream.WriteAsync(transmission, 0, transmission.Length);
                        count = 0;
                        transmission = new Byte[] { };
                    }
                }
                catch (Exception ex)
                {
                    log.Error($"Exception sending to ArcSight: {ex.Message}");
                }
            }
            if (count > 0)
            {
                try
                {
                    await stream.WriteAsync(transmission, 0, transmission.Length);
                }
                catch (Exception ex)
                {
                    log.Error($"Exception sending to ArcSight: {ex.Message}");
                }
            }
            await stream.FlushAsync();
        }

        public static bool ValidateMyCert(object sender, X509Certificate cert, X509Chain chain, SslPolicyErrors sslErr)
        {
            var splunkCertThumbprint = Util.GetEnvironmentVariable("splunkCertThumbprint");

            // if user has not configured a cert, anything goes
            if (splunkCertThumbprint == "")
                return true;

            // if user has configured a cert, must match
            var thumbprint = cert.GetCertHashString();
            if (thumbprint == splunkCertThumbprint)
                return true;

            return false;
        }

        static async Task obSplunk(string newClientContent, TraceWriter log)
        {
            //
            // newClientContent looks like this:
            //
            // {
            //   "records":[
            //     {...},
            //     {...}
            //     ...
            //   ]
            // }
            //

            string splunkAddress = Util.GetEnvironmentVariable("splunkAddress");
            string splunkToken = Util.GetEnvironmentVariable("splunkToken");

            if (splunkAddress.Length == 0 || splunkToken.Length == 0)
            {
                log.Error("Values for splunkAddress and splunkToken are required.");
                return;
            }

            ServicePointManager.Expect100Continue = true;
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            ServicePointManager.ServerCertificateValidationCallback += new RemoteCertificateValidationCallback(ValidateMyCert);

            var transmission = new StringBuilder();
            foreach (var message in convertToSplunk(newClientContent, null, log))
            {
                //
                // message looks like this:
                //
                // {
                //   "time": "xxx",
                //   "category": "xxx",
                //   "operationName": "xxx",
                //   "version": "xxx",
                //   "deviceExtId": "xxx",
                //   "flowOrder": "xxx",
                //   "nsgRuleName": "xxx",
                //   "dmac|smac": "xxx",
                //   "rt": "xxx",
                //   "src": "xxx",
                //   "dst": "xxx",
                //   "spt": "xxx",
                //   "dpt": "xxx",
                //   "proto": "xxx",
                //   "deviceDirection": "xxx",
                //   "act": "xxx"
                //  }
                transmission.Append(GetSplunkEventFromMessage(message));
            }

            var client = new SingleHttpClientInstance();
            try
            {
                HttpRequestMessage req = new HttpRequestMessage(HttpMethod.Post, splunkAddress);
                req.Headers.Accept.Clear();
                req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                req.Headers.Add("Authorization", "Splunk " + splunkToken);
                req.Content = new StringContent(transmission.ToString(), Encoding.UTF8, "application/json");
                HttpResponseMessage response = await SingleHttpClientInstance.SendToSplunk(req);
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    throw new System.Net.Http.HttpRequestException($"StatusCode from Splunk: {response.StatusCode}, and reason: {response.ReasonPhrase}");
                }
            }
            catch (System.Net.Http.HttpRequestException e)
            {
                throw new System.Net.Http.HttpRequestException("Sending to Splunk. Is Splunk service running?", e);
            }
            catch (Exception f)
            {
                throw new System.Exception("Sending to Splunk. Unplanned exception.", f);
            }

        }

        static System.Collections.Generic.IEnumerable<string> convertToSplunk(string newClientContent, Binder errorRecordBinder, TraceWriter log)
        {
            //
            // newClientContent looks like this:
            //
            // {
            //   "records":[
            //     {...},
            //     {...}
            //     ...
            //   ]
            // }
            //

            NSGFlowLogRecords logs = JsonConvert.DeserializeObject<NSGFlowLogRecords>(newClientContent);

            string logIncomingJSON = Util.GetEnvironmentVariable("logIncomingJSON");
            Boolean flag;
            if (Boolean.TryParse(logIncomingJSON, out flag))
            {
                if (flag)
                {
                    logErrorRecord(newClientContent, errorRecordBinder, log).Wait();
                }
            }

            var sbBase = new StringBuilder();
            foreach (var record in logs.records)
            {
                sbBase.Clear();
                sbBase.Append("{");
                sbBase.Append("\"time\":\"").Append(record.time).Append("\"");
                sbBase.Append(",\"category\":\"").Append(record.category).Append("\"");
                sbBase.Append(",\"operationName\":\"").Append(record.operationName).Append("\"");
                sbBase.Append(",\"version\":\"").Append(record.properties.Version.ToString("0.0")).Append("\"");
                sbBase.Append(",\"deviceExtId\":\"").Append(record.MakeDeviceExternalID()).Append("\"");

                int count = 1;
                var sbOuterFlowRecord = new StringBuilder();
                foreach (var outerFlows in record.properties.flows)
                {
                    sbOuterFlowRecord.Clear();
                    sbOuterFlowRecord.Append(sbBase.ToString());
                    sbOuterFlowRecord.Append(",\"flowOrder\":\"").Append(count).Append("\"");
                    sbOuterFlowRecord.Append(",\"nsgRuleName\":\"").Append(outerFlows.rule).Append("\"");

                    var sbInnerFlowRecord = new StringBuilder();
                    foreach (var innerFlows in outerFlows.flows)
                    {
                        sbInnerFlowRecord.Clear();
                        sbInnerFlowRecord.Append(sbOuterFlowRecord.ToString());

                        var firstFlowTupleEncountered = true;
                        foreach (var flowTuple in innerFlows.flowTuples)
                        {
                            var tuple = new NSGFlowLogTuple(flowTuple);

                            if (firstFlowTupleEncountered)
                            {
                                sbInnerFlowRecord.Append((tuple.GetDirection == "I" ? ",\"dmac\":\"" : ",\"smac\":\"")).Append(innerFlows.MakeMAC()).Append("\"");
                                firstFlowTupleEncountered = false;
                            }

                            yield return sbInnerFlowRecord.Append(tuple.JsonSubString()).ToString() + "}";
                        }

                    }
                }
            }
        }

        static StringBuilder sb = new StringBuilder();
        static string GetSplunkEventFromMessage(string message)
        {
            string json = Newtonsoft.Json.JsonConvert.SerializeObject(message);

            sb.Clear();
            sb.Append("{");
            sb.Append("\"sourcetype\": \"").Append("nsgFlowLog").Append("\",");
            sb.Append("\"event\": ").Append(json);
            sb.Append("}");

            return sb.ToString();

        }

        static Byte[] AppendToTransmission(Byte[] existingMessages, string appendMessage)
        {
            Byte[] appendMessageBytes = Encoding.ASCII.GetBytes(appendMessage);
            Byte[] crlf = new Byte[] { 0x0D, 0x0A };

            Byte[] newMessages = new Byte[existingMessages.Length + appendMessage.Length + 2];

            existingMessages.CopyTo(newMessages, 0);
            appendMessageBytes.CopyTo(newMessages, existingMessages.Length);
            crlf.CopyTo(newMessages, existingMessages.Length + appendMessageBytes.Length);

            return newMessages;
        }

        public class SingleHttpClientInstance
        {
            private static readonly HttpClient HttpClient;

            static SingleHttpClientInstance()
            {
                HttpClient = new HttpClient();
                HttpClient.Timeout = new TimeSpan(0, 1, 0);
            }

            public static async Task<HttpResponseMessage> SendToLogstash(HttpRequestMessage req, TraceWriter log)
            {
                HttpResponseMessage response = null;
                var httpClient = new HttpClient();
                httpClient.Timeout = TimeSpan.FromMinutes(5);
                try
                {
                    response = await httpClient.SendAsync(req);
                }
                catch (AggregateException ex)
                {
                    log.Error("Got AggregateException.");
                    throw ex;
                }
                catch (TaskCanceledException ex)
                {
                    log.Error("Got TaskCanceledException.");
                    throw ex;
                }
                catch (Exception ex)
                {
                    log.Error("Got other exception.");
                    throw ex;
                }
                return response;
            }

            public static async Task<HttpResponseMessage> SendToSplunk(HttpRequestMessage req)
            {
                HttpResponseMessage response = await HttpClient.SendAsync(req);
                return response;
            }

        }

        static async Task obLogstash(string newClientContent, TraceWriter log)
        {
            string logstashAddress = Util.GetEnvironmentVariable("logstashAddress");
            string logstashHttpUser = Util.GetEnvironmentVariable("logstashHttpUser");
            string logstashHttpPwd = Util.GetEnvironmentVariable("logstashHttpPwd");

            if (logstashAddress.Length == 0 || logstashHttpUser.Length == 0 || logstashHttpPwd.Length == 0)
            {
                log.Error("Values for logstashAddress, logstashHttpUser and logstashHttpPwd are required.");
                return;
            }

            ServicePointManager.Expect100Continue = true;
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            ServicePointManager.ServerCertificateValidationCallback =
            new System.Net.Security.RemoteCertificateValidationCallback(
                delegate { return true; });

            // log.Info($"newClientContent: {newClientContent}");

            var client = new SingleHttpClientInstance();
            var creds = Convert.ToBase64String(System.Text.ASCIIEncoding.ASCII.GetBytes(string.Format("{0}:{1}", logstashHttpUser, logstashHttpPwd)));
            try
            {
                HttpRequestMessage req = new HttpRequestMessage(HttpMethod.Post, logstashAddress);
                req.Headers.Accept.Clear();
                req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                req.Headers.Add("Authorization", "Basic " + creds);
                req.Content = new StringContent(newClientContent, Encoding.UTF8, "application/json");
                HttpResponseMessage response = await SingleHttpClientInstance.SendToLogstash(req, log);
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    log.Error($"StatusCode from Logstash: {response.StatusCode}, and reason: {response.ReasonPhrase}");
                }
            }
            catch (System.Net.Http.HttpRequestException e)
            {
                string msg = e.Message;
                if (e.InnerException != null)
                {
                    msg += " *** " + e.InnerException.Message;
                }
                log.Error($"HttpRequestException Error: \"{msg}\" was caught while sending to Logstash.");
                throw e;
            }
            catch (Exception f)
            {
                string msg = f.Message;
                if (f.InnerException != null)
                {
                    msg += " *** " + f.InnerException.Message;
                }
                log.Error($"Unknown error caught while sending to Logstash: \"{f.ToString()}\"");
                throw f;
            }
        }        
    }
}

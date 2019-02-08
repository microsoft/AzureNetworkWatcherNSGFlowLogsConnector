using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Newtonsoft.Json;
using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace NwNsgProject
{
    public partial class Util
    {
        public static async Task obSplunk(string newClientContent, TraceWriter log)
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
                float version = record.properties.Version;

                sbBase.Clear();
                sbBase.Append("{");

                sbBase.Append(eqs("time", true)).Append(eqs(record.time));
                sbBase.Append(eqs(true, "category")).Append(eqs(record.category));
                sbBase.Append(eqs(true, "operationName")).Append(eqs(record.operationName));
                sbBase.Append(eqs(true, "version")).Append(eqs(version.ToString("0.0")));
                sbBase.Append(eqs(true, "deviceExtId")).Append(eqs(record.MakeDeviceExternalID()));

                int count = 1;
                var sbOuterFlowRecord = new StringBuilder();
                foreach (var outerFlows in record.properties.flows)
                {
                    sbOuterFlowRecord.Clear();
                    sbOuterFlowRecord.Append(sbBase.ToString());
                    sbOuterFlowRecord.Append(eqs(true, "flowOrder")).Append(eqs(count.ToString()));
                    sbOuterFlowRecord.Append(eqs(true, "nsgRuleName")).Append(eqs(outerFlows.rule));

                    var sbInnerFlowRecord = new StringBuilder();
                    foreach (var innerFlows in outerFlows.flows)
                    {
                        sbInnerFlowRecord.Clear();
                        sbInnerFlowRecord.Append(sbOuterFlowRecord.ToString());

                        var firstFlowTupleEncountered = true;
                        foreach (var flowTuple in innerFlows.flowTuples)
                        {
                            var tuple = new NSGFlowLogTuple(flowTuple, version);

                            if (firstFlowTupleEncountered)
                            {
                                sbInnerFlowRecord.Append((tuple.GetDirection == "I" ? eqs(true, "dmac") : eqs(true, "smac"))).Append(eqs(innerFlows.MakeMAC()));
                                firstFlowTupleEncountered = false;
                            }

                            yield return sbInnerFlowRecord.ToString() + tuple.JsonSubString() + "}";
                        }

                    }
                }
            }
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

        static string GetSplunkEventFromMessage(string message)
        {
            StringBuilder sb = new StringBuilder();

            string json = Newtonsoft.Json.JsonConvert.SerializeObject(message);

            sb.Clear();
            sb.Append("{\"sourcetype\": \"nsgFlowLog\",\"event\": ").Append(json).Append("}");

            return sb.ToString();

        }


    }
}

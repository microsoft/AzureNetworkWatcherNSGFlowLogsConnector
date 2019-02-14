using Microsoft.Azure.WebJobs;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs.Host;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace NwNsgProject
{
    public partial class Util
    {
        const int MAXTRANSMISSIONSIZE = 255 * 1024;
//        const int MAXTRANSMISSIONSIZE = 2 * 1024;

        public static async Task obEventHub(string newClientContent, TraceWriter log)
        {
            string EventHubConnectionString = GetEnvironmentVariable("eventHubConnection");
            string EventHubName = GetEnvironmentVariable("eventHubName");
            if (EventHubConnectionString.Length == 0 || EventHubName.Length == 0)
            {
                log.Error("Values for eventHubConnection and eventHubName are required.");
                return;
            }

            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EventHubConnectionString)
            {
                EntityPath = EventHubName
            };
            var eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            foreach (var bundleOfMessages in bundleMessages(newClientContent, log))
            {
                //log.Info(String.Format("-----Outgoing message is: {0}", bundleOfMessages));

                await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(bundleOfMessages)));
            }
        }

        static System.Collections.Generic.IEnumerable<string> bundleMessages(string newClientContent, TraceWriter log)
        {
            var transmission = new StringBuilder(MAXTRANSMISSIONSIZE);
            transmission.Append("{\"records\":[");
            bool firstRecord = true;
            foreach (var message in denormalizeRecords(newClientContent, null, log))
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

                if (transmission.Length + message.Length > MAXTRANSMISSIONSIZE)
                {
                    transmission.Append("]}");
                    yield return transmission.ToString();
                    transmission.Clear();
                    transmission.Append("{\"records\":[");
                    firstRecord = true;
                }

                // add comma after existing transmission if it's not the first record
                if (firstRecord)
                {
                    firstRecord = false;
                }
                else
                {
                    transmission.Append(",");
                }

                transmission.Append(message);
            }
            if (transmission.Length > 0)
            {
                transmission.Append("]}");
                yield return transmission.ToString();
            }
        }

        static System.Collections.Generic.IEnumerable<string> denormalizeRecords(string newClientContent, Binder errorRecordBinder, TraceWriter log)
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

            var sbBase = new StringBuilder(500);
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
    }
}

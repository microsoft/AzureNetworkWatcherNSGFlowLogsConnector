using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Newtonsoft.Json;
using System;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace NwNsgProject
{
    public partial class Util
    {
        public static async Task obArcsight(string newClientContent, TraceWriter log)
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
                    transmission = Util.AppendToTransmission(transmission, message);

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

        public static System.Collections.Generic.IEnumerable<string> convertToCEF(string newClientContent, Binder errorRecordBinder, TraceWriter log)
        {
            // newClientContent is a json string with records

            NSGFlowLogRecords logs = JsonConvert.DeserializeObject<NSGFlowLogRecords>(newClientContent);

            string logIncomingJSON = Util.GetEnvironmentVariable("logIncomingJSON");
            Boolean flag;
            if (Boolean.TryParse(logIncomingJSON, out flag))
            {
                if (flag)
                {
                    Util.logErrorRecord(newClientContent, errorRecordBinder, log).Wait();
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

    }
}

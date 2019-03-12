using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Buffers;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace nsgFunc
{
    public partial class Util
    {
        public static async Task<int> obArcsight(string newClientContent, ExecutionContext executionContext, ILogger log)
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
                log.LogError("Values for arcsightAddress and arcsightPort are required.");
                return 0;
            }

            TcpClient client = new TcpClient(arcsightAddress, Convert.ToInt32(arcsightPort));
            NetworkStream stream = client.GetStream();

            int count = 0;
            int bytesSent = 0;
            Byte[] transmission = new Byte[] { };

            System.Diagnostics.Stopwatch sw = System.Diagnostics.Stopwatch.StartNew();
            foreach (var message in convertToCEF(newClientContent, null, log))
            {
                try
                {
                    transmission = Util.AppendToTransmission(transmission, message);

                    // batch up the messages
                    if (count++ == 1000)
                    {
                        sw.Stop();
                        log.LogDebug($"Time to build new transmission byte[] from convertToCEF: {sw.ElapsedMilliseconds}");

                        sw.Reset();
                        sw.Start();

                        await stream.WriteAsync(transmission, 0, transmission.Length);
                        bytesSent += transmission.Length;

                        sw.Stop();
                        log.LogDebug($"Time to transmit to ArcSight server: {sw.ElapsedMilliseconds}");

                        sw.Reset();
                        sw.Start();

                        count = 0;
                        transmission = new Byte[] { };
                    }
                }
                catch (Exception ex)
                {
                    log.LogError($"Exception sending to ArcSight: {ex.Message}");
                }
            }
            if (count > 0)
            {
                try
                {
                    await stream.WriteAsync(transmission, 0, transmission.Length);
                    bytesSent += transmission.Length;

                    sw.Stop();
                    log.LogDebug($"Time to transmit to ArcSight server: {sw.ElapsedMilliseconds}");

                }
                catch (Exception ex)
                {
                    log.LogError($"Exception sending to ArcSight: {ex.Message}");
                }
            }
            await stream.FlushAsync();

            return bytesSent;
        }

        public static async Task<int> obArcsightNew(string newClientContent, ExecutionContext executionContext, ILogger log)
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
                log.LogError("Values for arcsightAddress and arcsightPort are required.");
                return 0;
            }

            TcpClient client = new TcpClient(arcsightAddress, Convert.ToInt32(arcsightPort));
            NetworkStream stream = client.GetStream();

            int transmittedByteCount = 0;
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            foreach (var tuple in bundleMessageListsCEF(newClientContent, log))
            {
                try
                {
                    sw.Start();
                    await stream.WriteAsync(tuple.Item1, 0, tuple.Item2);
                    sw.Stop();

                    transmittedByteCount += tuple.Item2;

                    log.LogDebug($"Transmitted {tuple.Item2} bytes, in time {sw.ElapsedMilliseconds}, operation id {executionContext.InvocationId}.");
                    sw.Reset();

                }
                catch (Exception ex)
                {
                    log.LogError($"Exception sending to ArcSight: {ex.Message}");
                }
            }
            await stream.FlushAsync();

            return transmittedByteCount;
        }

        static System.Collections.Generic.IEnumerable<Tuple<byte[],int>> bundleMessageListsCEF(string newClientContent, ILogger log)
        {
            const int MAXBUFFERSIZE = 1024 * 1024;
            var bytePool = ArrayPool<byte>.Shared;
            byte[] transmission = bytePool.Rent((int)MAXBUFFERSIZE);
            int transmissionLength = 0;

            try
            {
                System.Diagnostics.Stopwatch sw = System.Diagnostics.Stopwatch.StartNew();
                foreach (var messageList in denormalizedRecords(newClientContent, null, log))
                {
                    sw.Stop();
                    log.LogDebug($"Time to get new messageList from denormalizedRecords: {sw.ElapsedMilliseconds}");

                    sw.Reset();
                    sw.Start();
                    foreach (var message in messageList)
                    {
                        int bytesAppended = message.AppendToTransmission(ref transmission, MAXBUFFERSIZE, transmissionLength);

                        transmissionLength += bytesAppended;
                    }
                    sw.Stop();
                    log.LogDebug($"Time to build transmission from messageList: {sw.ElapsedMilliseconds}");
                    yield return Tuple.Create(transmission, transmissionLength);

                    sw.Reset();
                    sw.Start();
                    transmissionLength = 0;
                }
            }
            finally
            {
                bytePool.Return(transmission);
            }
        }

        public static System.Collections.Generic.IEnumerable<string> convertToCEF(string newClientContent, Binder errorRecordBinder, ILogger log)
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

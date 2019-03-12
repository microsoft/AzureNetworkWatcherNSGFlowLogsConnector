using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Buffers;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace nsgFunc
{
    public partial class Util
    {
        public static async Task<int> obArcsightNew(string newClientContent, ExecutionContext executionContext, Binder cefLogBinder, ILogger log)
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

            string logOutgoingCEF = Util.GetEnvironmentVariable("logOutgoingCEF");
            Boolean logOutgoingCEFflag;
            Boolean.TryParse(logOutgoingCEF, out logOutgoingCEFflag);

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

                    if (logOutgoingCEFflag) { 
                        Guid guid = Guid.NewGuid();
                        var attributes = new Attribute[]
                        {
                                new BlobAttribute(String.Format("ceflog/{0}", guid)),
                                new StorageAccountAttribute("cefLogAccount")
                        };

                        CloudBlockBlob blob = await cefLogBinder.BindAsync<CloudBlockBlob>(attributes);
                        await blob.UploadFromByteArrayAsync(tuple.Item1, 0, tuple.Item2);
                    }

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
    }
}

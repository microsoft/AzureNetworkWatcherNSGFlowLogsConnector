using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.Host.Bindings;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Threading.Tasks;

namespace NwNsgProject
{
    public static class Stage2QueueTrigger
    {
        const int MAX_CHUNK_SIZE = 102400;

        [FunctionName("Stage2QueueTrigger")]
        public static async Task Run(
            [QueueTrigger("stage1", Connection = "AzureWebJobsStorage")]Chunk inputChunk,
            [Queue("stage2", Connection = "AzureWebJobsStorage")] ICollector<Chunk> outputQueue,
            Binder binder,
            TraceWriter log)
        {
//            log.Info($"C# Queue trigger function processed: {inputChunk}");

            if (inputChunk.Length < MAX_CHUNK_SIZE)
            {
                outputQueue.Add(inputChunk);
                return;
            }

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

            byte[] nsgMessages = new byte[inputChunk.Length];
            try
            {
                CloudBlockBlob blob = await binder.BindAsync<CloudBlockBlob>(attributes);
                await blob.DownloadRangeToByteArrayAsync(nsgMessages, 0, inputChunk.Start, inputChunk.Length);
            }
            catch (Exception ex)
            {
                log.Error(string.Format("Error binding blob input: {0}", ex.Message));
                throw ex;
            }

            int startingByte = 0;
            var chunkCount = 0;

            var newChunk = GetNewChunk(inputChunk, chunkCount++, log, 0);

            //long length = FindNextRecord(nsgMessages, startingByte);
            var nsgMessagesString = System.Text.Encoding.Default.GetString(nsgMessages);
            int endingByte = FindNextRecordRecurse(nsgMessagesString, startingByte, 0, log);
            int length = endingByte - startingByte + 1;

            while (length != 0)
            {
                if (newChunk.Length + length > MAX_CHUNK_SIZE)
                {
                    //log.Info($"Chunk starts at {newChunk.Start}, length is {newChunk.Length}, next start is {newChunk.Start + newChunk.Length}");
                    outputQueue.Add(newChunk);

                    newChunk = GetNewChunk(inputChunk, chunkCount++, log, newChunk.Start + newChunk.Length);
                }

                newChunk.Length += length;
                startingByte += length;

                endingByte = FindNextRecordRecurse(nsgMessagesString, startingByte, 0, log);
                length = endingByte - startingByte + 1;
            }

            if (newChunk.Length > 0)
            {
                outputQueue.Add(newChunk);
                //log.Info($"Chunk starts at {newChunk.Start}, length is {newChunk.Length}");
            }

        }

        public static Chunk GetNewChunk(Chunk thisChunk, int index, TraceWriter log, long start = 0)
        {
            var chunk = new Chunk
            {
                BlobName = thisChunk.BlobName,
                BlobAccountConnectionName = thisChunk.BlobAccountConnectionName,
                LastBlockName = string.Format("{0}-{1}", index, thisChunk.LastBlockName),
                Start = (start == 0 ? thisChunk.Start : start),
                Length = 0
            };

            //log.Info($"new chunk: {chunk.ToString()}");

            return chunk;
        }

        public static long FindNextRecord(byte[] array, long startingByte)
        {
            var arraySize = array.Length;
            var endingByte = startingByte;
            var curlyBraceCount = 0;
            var insideARecord = false;

            for (long index = startingByte; index < arraySize; index++)
            {
                endingByte++;

                if (array[index] == '{')
                {
                    insideARecord = true;
                    curlyBraceCount++;
                }

                curlyBraceCount -= (array[index] == '}' ? 1 : 0);

                if (insideARecord && curlyBraceCount == 0)
                {
                    break;
                }
            }

            return endingByte - startingByte;
        }

        public static int FindNextRecordRecurse(string nsgMessages, int startingByte, int braceCounter, TraceWriter log)
        {
            if (startingByte == nsgMessages.Length)
            {
                return startingByte - 1;
            }

            int nextBrace = nsgMessages.IndexOfAny(new Char[] { '}', '{' }, startingByte);

            if (nsgMessages[nextBrace] == '{')
            {
                braceCounter++;
                nextBrace = FindNextRecordRecurse(nsgMessages, nextBrace + 1, braceCounter, log);
            }
            else
            {
                braceCounter--;
                if (braceCounter > 0)
                {
                    nextBrace = FindNextRecordRecurse(nsgMessages, nextBrace + 1, braceCounter, log);
                }
            }

            return nextBrace;
        }
    }


}

using System.IO;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;

namespace NwNsgProject
{
    public static class Stage1BlobTrigger
    {
        const int MAXDOWNLOADBYTES = 102400;

        [FunctionName("stage1BlobTrigger")]
        public static void Run(
            [BlobTrigger("%blobContainerName%/resourceId=/SUBSCRIPTIONS/{subId}/RESOURCEGROUPS/{resourceGroup}/PROVIDERS/MICROSOFT.NETWORK/NETWORKSECURITYGROUPS/{nsgName}/y={blobYear}/m={blobMonth}/d={blobDay}/h={blobHour}/m={blobMinute}/macAddress={mac}/PT1H.json", Connection = "%nsgSourceDataAccount%")]CloudBlockBlob myBlob,
            [Queue("stage1", Connection = "AzureWebJobsStorage")] ICollector<Chunk> outputChunks,
            [Table("checkpoints", Connection = "AzureWebJobsStorage")] CloudTable checkpointTable,
            string subId, string resourceGroup, string nsgName, string blobYear, string blobMonth, string blobDay, string blobHour, string blobMinute, string mac,
            TraceWriter log)
        {
            string nsgSourceDataAccount = Util.GetEnvironmentVariable("nsgSourceDataAccount");
            if (nsgSourceDataAccount.Length == 0)
            {
                log.Error("Value for nsgSourceDataAccount is required.");
                throw new System.ArgumentNullException("nsgSourceDataAccount", "Please provide setting.");
            }

            string blobContainerName = Util.GetEnvironmentVariable("blobContainerName");
            if (blobContainerName.Length == 0)
            {
                log.Error("Value for blobContainerName is required.");
                throw new System.ArgumentNullException("blobContainerName", "Please provide setting.");
            }

            var blobDetails = new BlobDetails(subId, resourceGroup, nsgName, blobYear, blobMonth, blobDay, blobHour, blobMinute, mac);

            // get checkpoint
            Checkpoint checkpoint = Checkpoint.GetCheckpoint(blobDetails, checkpointTable);

            // break up the block list into 10k chunks
            List<Chunk> chunks = new List<Chunk>();
            long currentChunkSize = 0;
            string currentChunkLastBlockName = "";
            long currentStartingByteOffset = 0;

            bool firstBlockItem = true;
            bool foundStartingOffset = false;
            bool tieOffChunk = false;

            int numberOfBlocks = 0;
            long sizeOfBlocks = 0;

            foreach (var blockListItem in myBlob.DownloadBlockList(BlockListingFilter.Committed))
            {
                if (!foundStartingOffset)
                {
                    if (firstBlockItem)
                    {
                        currentStartingByteOffset += blockListItem.Length;
                        firstBlockItem = false;
                        if (checkpoint.LastBlockName == "")
                        {
                            foundStartingOffset = true;
                        }
                    }
                    else
                    {
                        if (blockListItem.Name == checkpoint.LastBlockName)
                        {
                            foundStartingOffset = true;
                        }
                        currentStartingByteOffset += blockListItem.Length;
                    }
                }
                else
                {
                    // tieOffChunk = add current chunk to the list, initialize next chunk counters
                    // conditions to account for:
                    // 1) current chunk is empty & not the last block (size > 10 I think)
                    //   a) add blockListItem to current chunk
                    //   b) loop
                    // 2) current chunk is empty & last block (size < 10 I think)
                    //   a) do not add blockListItem to current chunk
                    //   b) loop terminates
                    //   c) chunk last added to the list is the last chunk
                    // 3) current chunk is not empty & not the last block
                    //   a) if size of block + size of chunk >10k
                    //     i) add chunk to list  <-- tieOffChunk
                    //     ii) reset chunk counters
                    //   b) add blockListItem to chunk
                    //   c) loop
                    // 4) current chunk is not empty & last block
                    //   a) add chunk to list  <-- tieOffChunk
                    //   b) do not add blockListItem to chunk
                    //   c) loop terminates
                    tieOffChunk = (currentChunkSize != 0) && ((blockListItem.Length < 10) || (currentChunkSize + blockListItem.Length > MAXDOWNLOADBYTES));
                    if (tieOffChunk)
                    {
                        // chunk complete, add it to the list & reset counters
                        chunks.Add(new Chunk
                        {
                            BlobName = blobContainerName + "/" + myBlob.Name,
                            Length = currentChunkSize,
                            LastBlockName = currentChunkLastBlockName,
                            Start = currentStartingByteOffset,
                            BlobAccountConnectionName = nsgSourceDataAccount
                        });
                        currentStartingByteOffset += currentChunkSize; // the next chunk starts at this offset
                        currentChunkSize = 0;
                        tieOffChunk = false;
                    }
                    if (blockListItem.Length > 10)
                    {
                        numberOfBlocks++;
                        sizeOfBlocks += blockListItem.Length;

                        currentChunkSize += blockListItem.Length;
                        currentChunkLastBlockName = blockListItem.Name;
                    }
                }

            }
            if (currentChunkSize != 0)
            {
                // residual chunk
                chunks.Add(new Chunk
                {
                    BlobName = blobContainerName + "/" + myBlob.Name,
                    Length = currentChunkSize,
                    LastBlockName = currentChunkLastBlockName,
                    Start = currentStartingByteOffset,
                    BlobAccountConnectionName = nsgSourceDataAccount
                });
            }

            if (chunks.Count > 0)
            {
                var lastChunk = chunks[chunks.Count - 1];
                checkpoint.PutCheckpoint(checkpointTable, lastChunk.LastBlockName, lastChunk.Start + lastChunk.Length);
            }

            // add the chunks to output queue
            // they are sent automatically by Functions configuration
            foreach (var chunk in chunks)
            {
                outputChunks.Add(chunk);
                if (chunk.Length == 0)
                {
                    log.Error("chunk length is 0");
                }
            }

        }
    }

}

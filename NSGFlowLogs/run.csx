#load "../shared/getEnvironmentVariable.csx"
#load "../shared/chunk.csx"
#load "../shared/blobname.csx"
#load "../shared/checkpoint.csx"

#r "Microsoft.WindowsAzure.Storage"

using System;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

const int MAXDOWNLOADBYTES = 10240;

//public static async Task Run(CloudBlockBlob myBlob, CloudTable checkpointTable, ICollector<Chunk> outputChunks, string subId, string resourceGroup, string nsgName, string blobYear, string blobMonth, string blobDay, string blobHour, string mac, TraceWriter log)
public static async Task Run(CloudBlockBlob myBlob, CloudTable checkpointTable, ICollector<Chunk> outputChunks, string subId, string resourceGroup, string nsgName, string blobYear, string blobMonth, string blobDay, string blobHour, TraceWriter log)
{
    string mac = "none";
    var blobName = new BlobName(subId, resourceGroup, nsgName, blobYear, blobMonth, blobDay, blobHour, mac);
//    var blobName = new BlobName(subId, resourceGroup, nsgName, blobYear, blobMonth, blobDay, blobHour);

    string nsgSourceDataAccount = getEnvironmentVariable("nsgSourceDataAccount");
    if (nsgSourceDataAccount.Length == 0)
    {
        log.Error("Value for nsgSourceDataAccount is required.");
        return;
    }

    string blobContainerName = getEnvironmentVariable("blobContainerName");
    if (blobContainerName.Length == 0)
    {
        log.Error("Value for blobContainerName is required.");
        return;
    }

    //    add this to the binding in function.json:  /macAddress={mac}

    // get checkpoint
    Checkpoint checkpoint = GetCheckpoint(blobName, checkpointTable, log);

    //// break up the block list into 10k chunks
    List<Chunk> chunks = new List<Chunk>();
    long currentChunkSize = 0;
    string currentChunkLastBlockName = "";
    long currentStartingByteOffset = 0;

    bool firstBlockItem = true;
    bool foundStartingOffset = false;
    bool tieOffChunk = false;
    string msg;
    //msg = string.Format("Current checkpoint last block name: {0}", checkpoint.LastBlockName);
    //log.Info(msg);
    foreach (var blockListItem in myBlob.DownloadBlockList(BlockListingFilter.Committed))
    {
        //msg = string.Format("Block name: {0}, length: {1}", blockListItem.Name, blockListItem.Length);
        //log.Info(msg);

        // skip first block, but add to the offset
        // find the starting block based on checkpoint LastBlockName
        // if it's empty, start at the beginning
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
            tieOffChunk = (currentChunkSize !=0) && ((blockListItem.Length < 10) || (currentChunkSize + blockListItem.Length > MAXDOWNLOADBYTES));
            if (tieOffChunk)
            {
                // chunk complete, add it to the list & reset counters
                chunks.Add(new Chunk {
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

    // debug logging
    //msg = string.Format("Chunks to download & transmit: {0}", chunks.Count);
    //log.Info(msg);
    //foreach (var chunk in chunks)
    //{
    //    msg = string.Format("Starting byte offset: {0}, size of chunk: {1}, name of last block: {2}", chunk.Start, chunk.Length, chunk.LastBlockName);
    //    log.Info(msg);
    //}

    // update the checkpoint
    if (chunks.Count > 0)
    {
        var lastChunk = chunks[chunks.Count - 1];
        PutCheckpoint(blobName, checkpointTable, lastChunk.LastBlockName, lastChunk.Start + lastChunk.Length, log);
    }

    // add the chunks to output queue
    // they are sent automatically by Functions configuration
    foreach (var chunk in chunks)
    {
        outputChunks.Add(chunk);
        if (chunk.Length == 0)
        {
            log.Info("chunk length is 0");
        }
    }
}


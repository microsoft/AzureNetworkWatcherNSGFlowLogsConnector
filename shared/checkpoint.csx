#r "Microsoft.WindowsAzure.Storage"

using System;
using Microsoft.WindowsAzure.Storage.Table;

public static Checkpoint GetCheckpoint(BlobName blobName, CloudTable checkpointTable, TraceWriter log)
{

    var checkpointPartitionKey = blobName.GetPartitionKey();
    var checkpointRowKey = blobName.GetRowKey();

    Checkpoint checkpoint = null;
    try
    {
        TableOperation operation = TableOperation.Retrieve<Checkpoint>(checkpointPartitionKey, checkpointRowKey);
        TableResult result = checkpointTable.Execute(operation);
        checkpoint = (Checkpoint)result.Result;

        if (checkpoint == null)
        {
            checkpoint = new Checkpoint(checkpointPartitionKey, checkpointRowKey, "", 0);
        }
    }
    catch (Exception ex)
    {
        var msg = string.Format("Error fetching checkpoint for blob: {0}", ex.Message);
        log.Info(msg);
    }

    return checkpoint;

}

public static void PutCheckpoint(BlobName blobName, CloudTable checkpointTable, string lastBlockName, long startingByteOffset, TraceWriter log)
{
    var newCheckpoint = new Checkpoint(
        blobName.GetPartitionKey(), blobName.GetRowKey(),
        lastBlockName, startingByteOffset);
    TableOperation operation = TableOperation.InsertOrReplace(newCheckpoint);
    checkpointTable.Execute(operation);
}

public class Checkpoint : TableEntity, IDisposable
{
    public Checkpoint() { }

    public Checkpoint(string partitionKey, string rowKey, string blockName, long offset)
    {
        PartitionKey = partitionKey;
        RowKey = rowKey;
        LastBlockName = blockName;
        StartingByteOffset = offset;
    }

    public string LastBlockName { get; set; }
    public long StartingByteOffset { get; set; }

    bool disposed = false;
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposed)
            return;

        if (disposing)
        {
        }

        disposed = true;
    }
}

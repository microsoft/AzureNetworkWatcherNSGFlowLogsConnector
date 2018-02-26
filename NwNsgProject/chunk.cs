using System;

namespace NwNsgProject
{
    public class Chunk
    {
        public string BlobName { get; set; }
        public string BlobAccountConnectionName { get; set; }
        public long Length { get; set; }
        public long Start { get; set; }
        public string LastBlockName { get; set; }

        public override string ToString()
        {
            var msg = string.Format("Connection: {0}, Block: {1}, Start: {2}, Length: {3}, Name: {4}", BlobAccountConnectionName, LastBlockName, Start, Length, BlobName);
            return msg;
        }
    }
}





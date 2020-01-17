public class Source
{
    public string address { get; set; }
    public string ip { get; set; }
    public string port { get; set; }
    public string packets { get; set; }
    public string bytes { get; set; }
    
    public Source(DenormalizedRecord denormalizedRecord)
    {
        this.address = denormalizedRecord.sourceAddress;
        this.ip = denormalizedRecord.sourceAddress;
        this.port = denormalizedRecord.sourcePort;
        this.packets = denormalizedRecord.packetsStoD;
        this.bytes = denormalizedRecord.bytesStoD;
    }
}
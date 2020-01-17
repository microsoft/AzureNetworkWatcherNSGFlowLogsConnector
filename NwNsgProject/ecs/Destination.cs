public class Destination
{
    public string address { get; set; }
    public string ip { get; set; }
    public string port { get; set; }
    public string packets { get; set; }
    public string bytes { get; set; }
    
    public Destination(DenormalizedRecord denormalizedRecord)
    {
        this.address = denormalizedRecord.destinationAddress;
        this.ip = denormalizedRecord.destinationAddress;
        this.port = denormalizedRecord.destinationPort;
        this.packets = denormalizedRecord.packetsDtoS;
        this.bytes = denormalizedRecord.bytesDtoS;
    }
}
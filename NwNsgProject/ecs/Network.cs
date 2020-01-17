using System;
public class Network
{
    public string transport { get; set; }
    public string direction { get; set; }
    public string protocol { get; set; }
    public string bytes { get; set; }
    public string packets { get; set; }
    public string flowstate { get; set; }
    public Network(DenormalizedRecord denormalizedRecord)
    {
        this.transport = (denormalizedRecord.transportProtocol == "U" ? "udp" : "tcp");
        this.direction = (denormalizedRecord.deviceDirection == "I" ? "inbound" : "outbound");
        this.protocol = "transport";

        this.bytes =   (Convert.ToInt64(denormalizedRecord.bytesStoD) + Convert.ToInt64(denormalizedRecord.bytesDtoS)).ToString();
        this.packets = (Convert.ToInt64(denormalizedRecord.packetsStoD) + Convert.ToInt64(denormalizedRecord.packetsDtoS)).ToString();

        this.flowstate = denormalizedRecord.flowState;
    }
}
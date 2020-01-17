using System;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using System.Buffers;
using Microsoft.CodeAnalysis.Formatting;
public class NSGFlowLogTuple
{
    public float schemaVersion { get; set; }

    public string startTime { get; set; }
    public string sourceAddress { get; set; }
    public string destinationAddress { get; set; }
    public string sourcePort { get; set; }
    public string destinationPort { get; set; }
    public string transportProtocol { get; set; }
    public string deviceDirection { get; set; }
    public string deviceAction { get; set; }

    // version 2 tuple properties
    public string flowState { get; set; }
    public string packetsStoD { get; set; }
    public string bytesStoD { get; set; }
    public string packetsDtoS { get; set; }
    public string bytesDtoS { get; set; }

    public NSGFlowLogTuple(string tuple, float version)
    {
        schemaVersion = version;

        char[] sep = new char[] { ',' };
        string[] parts = tuple.Split(sep);
        startTime = parts[0];
        sourceAddress = parts[1];
        destinationAddress = parts[2];
        sourcePort = parts[3];
        destinationPort = parts[4];
        transportProtocol = parts[5];
        deviceDirection = parts[6];
        deviceAction = parts[7];

        if (version >= 2.0)
        {
            flowState = parts[8];
            if (flowState != "B")
            {
                packetsStoD = (parts[9] == "" ? "0" : parts[9]);
                bytesStoD = (parts[10] == "" ? "0" : parts[10]);
                packetsDtoS = (parts[11] == "" ? "0" : parts[11]);
                bytesDtoS = (parts[12] == "" ? "0" : parts[12]);
            }
        }
    }

    public string GetDirection
    {
        get { return deviceDirection; }
    }

    public override string ToString()
    {
        var temp = new StringBuilder();
        temp.Append("rt=").Append((Convert.ToUInt64(startTime) * 1000).ToString());
        temp.Append(" src=").Append(sourceAddress);
        temp.Append(" dst=").Append(destinationAddress);
        temp.Append(" spt=").Append(sourcePort);
        temp.Append(" dpt=").Append(destinationPort);
        temp.Append(" proto=").Append((transportProtocol == "U" ? "UDP" : "TCP"));
        temp.Append(" deviceDirection=").Append((deviceDirection == "I" ? "0" : "1"));
        temp.Append(" act=").Append(deviceAction);

        if (schemaVersion >= 2.0)
        {
            // add fields from version 2 schema
            temp.Append(" cs2=").Append(flowState);
            temp.Append(" cs2Label=FlowState");

            if (flowState != "B")
            {
                temp.Append(" cn1=").Append(packetsStoD);
                temp.Append(" cn1Label=PacketsStoD");
                temp.Append(" cn2=").Append(packetsDtoS);
                temp.Append(" cn2Label=PacketsDtoS");

                if (deviceDirection == "I")
                {
                    temp.Append(" bytesIn=").Append(bytesStoD);
                    temp.Append(" bytesOut=").Append(bytesDtoS);
                }
                else
                {
                    temp.Append(" bytesIn=").Append(bytesDtoS);
                    temp.Append(" bytesOut=").Append(bytesStoD);
                }
            }
        }

        return temp.ToString();
    }

    public string JsonSubString()
    {
        var sb = new StringBuilder();
        sb.Append(",\"rt\":\"").Append((Convert.ToUInt64(startTime) * 1000).ToString()).Append("\"");
        sb.Append(",\"src\":\"").Append(sourceAddress).Append("\"");
        sb.Append(",\"dst\":\"").Append(destinationAddress).Append("\"");
        sb.Append(",\"spt\":\"").Append(sourcePort).Append("\"");
        sb.Append(",\"dpt\":\"").Append(destinationPort).Append("\"");
        sb.Append(",\"proto\":\"").Append((transportProtocol == "U" ? "UDP" : "TCP")).Append("\"");
        sb.Append(",\"deviceDirection\":\"").Append((deviceDirection == "I" ? "0" : "1")).Append("\"");
        sb.Append(",\"act\":\"").Append(deviceAction).Append("\"");

        return sb.ToString();
    }
}

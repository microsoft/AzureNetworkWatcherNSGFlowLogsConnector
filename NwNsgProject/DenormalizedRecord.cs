using System;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using System.Buffers;
using Microsoft.CodeAnalysis.Formatting;
public class DenormalizedRecord
{
    public string time { get; set; }
    public string category { get; set; }
    public string operationName { get; set; }
    public string resourceId { get; set; }
    public float version { get; set; }
    public string deviceExtId { get; set; }
    public string nsgRuleName { get; set; }
    public string mac { get; set; }
    public string startTime { get; set; }
    public string sourceAddress { get; set; }
    public string destinationAddress { get; set; }
    public string sourcePort { get; set; }
    public string destinationPort { get; set; }
    public string transportProtocol { get; set; }
    public string deviceDirection { get; set; }
    public string deviceAction { get; set; }
    public string flowState { get; set; }
    public string packetsStoD { get; set; }
    public string bytesStoD { get; set; }
    public string packetsDtoS { get; set; }
    public string bytesDtoS { get; set; }

    public DenormalizedRecord(
        float version,
        string time,
        string category,
        string operationName,
        string resourceId,
        string nsgRuleName,
        string mac,
        NSGFlowLogTuple tuple
        )
    {
        this.version = version;
        this.time = time;
        this.category = category;
        this.operationName = operationName;
        this.resourceId = resourceId;
        this.nsgRuleName = nsgRuleName;
        this.mac = mac;
        this.startTime = tuple.startTime;
        this.sourceAddress = tuple.sourceAddress;
        this.destinationAddress = tuple.destinationAddress;
        this.sourcePort = tuple.sourcePort;
        this.destinationPort = tuple.destinationPort;
        this.transportProtocol = tuple.transportProtocol;
        this.deviceDirection = tuple.deviceDirection;
        this.deviceAction = tuple.deviceAction;
        if (this.version >= 2.0)
        {
            this.flowState = tuple.flowState;
            this.packetsDtoS = tuple.packetsDtoS;
            this.packetsStoD = tuple.packetsStoD;
            this.bytesDtoS = tuple.bytesDtoS;
            this.bytesStoD = tuple.bytesStoD;
        }
    }

    private string MakeMAC()
    {
        StringBuilder sb = StringBuilderPool.Allocate();
        string delimitedMac = "";
        try
        {
            sb.Append(mac.Substring(0, 2)).Append(":");
            sb.Append(mac.Substring(2, 2)).Append(":");
            sb.Append(mac.Substring(4, 2)).Append(":");
            sb.Append(mac.Substring(6, 2)).Append(":");
            sb.Append(mac.Substring(8, 2)).Append(":");
            sb.Append(mac.Substring(10, 2));

            delimitedMac = sb.ToString();
        }
        finally
        {
            StringBuilderPool.Free(sb);
        }


        return delimitedMac;
    }

    private string MakeDeviceExternalID()
    {
        var patternSubscriptionId = "SUBSCRIPTIONS\\/(.*?)\\/";
        var patternResourceGroup = "SUBSCRIPTIONS\\/(?:.*?)\\/RESOURCEGROUPS\\/(.*?)\\/";
        var patternResourceName = "PROVIDERS\\/(?:.*?\\/.*?\\/)(.*?)(?:\\/|$)";

        Match m = Regex.Match(resourceId, patternSubscriptionId);
        var subscriptionID = m.Groups[1].Value;

        m = Regex.Match(resourceId, patternResourceGroup);
        var resourceGroup = m.Groups[1].Value;

        m = Regex.Match(resourceId, patternResourceName);
        var resourceName = m.Groups[1].Value;

        return subscriptionID + "/" + resourceGroup + "/" + resourceName;
    }

    private string MakeCEFTime()
    {
        // sample input: "2017-08-09T00:13:25.4850000Z"
        // sample output: Aug 09 00:13:25 host CEF:0

        CultureInfo culture = new CultureInfo("en-US");
        DateTime tempDate = Convert.ToDateTime(time, culture);
        string newTime = tempDate.ToString("MMM dd HH:mm:ss");

        return newTime + " host CEF:0";
    }


    private void BuildCEF(ref StringBuilder sb)
    {
        sb.Append(MakeCEFTime());
        sb.Append("|Microsoft.Network");
        sb.Append("|NETWORKSECURITYGROUPS");
        sb.Append("|").Append(version.ToString("0.0"));
        sb.Append("|").Append(category);
        sb.Append("|").Append(operationName);
        sb.Append("|1");  // severity is always 1
        sb.Append("|deviceExternalId=").Append(MakeDeviceExternalID());

        sb.Append(String.Format(" cs1={0}", nsgRuleName));
        sb.Append(String.Format(" cs1Label=NSGRuleName"));

        sb.Append((deviceDirection == "I" ? " dmac=" : " smac=") + MakeMAC());

        sb.Append(" rt=").Append((Convert.ToUInt64(startTime) * 1000).ToString());
        sb.Append(" src=").Append(sourceAddress);
        sb.Append(" dst=").Append(destinationAddress);
        sb.Append(" spt=").Append(sourcePort);
        sb.Append(" dpt=").Append(destinationPort);
        sb.Append(" proto=").Append((transportProtocol == "U" ? "UDP" : "TCP"));
        sb.Append(" deviceDirection=").Append((deviceDirection == "I" ? "0" : "1"));
        sb.Append(" act=").Append(deviceAction);

        if (version >= 2.0)
        {
            // add fields from version 2 schema
            sb.Append(" cs2=").Append(flowState);
            sb.Append(" cs2Label=FlowState");

            if (flowState != "B")
            {
                sb.Append(" cn1=").Append(packetsStoD);
                sb.Append(" cn1Label=PacketsStoD");
                sb.Append(" cn2=").Append(packetsDtoS);
                sb.Append(" cn2Label=PacketsDtoS");

                if (deviceDirection == "I")
                {
                    sb.Append(" bytesIn=").Append(bytesStoD);
                    sb.Append(" bytesOut=").Append(bytesDtoS);
                }
                else
                {
                    sb.Append(" bytesIn=").Append(bytesDtoS);
                    sb.Append(" bytesOut=").Append(bytesStoD);
                }
            }
        }
    }

    public int AppendToTransmission(ref byte[] transmission, int maxSize, int offset)
    {
        StringBuilder sb = StringBuilderPool.Allocate();
        var bytePool = ArrayPool<byte>.Shared;
        byte[] buffer = bytePool.Rent((int)1000);
        byte[] crlf = new Byte[] { 0x0D, 0x0A };
        int bytesToAppend = 0;

        try
        {
            BuildCEF(ref sb);

            string s = sb.ToString();
            bytesToAppend += s.Length + 2;

            if (maxSize > offset + bytesToAppend)
            {
                Buffer.BlockCopy(Encoding.ASCII.GetBytes(s), 0, buffer, 0, s.Length);
                Buffer.BlockCopy(crlf, 0, buffer, s.Length, 2);

                Buffer.BlockCopy(buffer, 0, transmission, offset, bytesToAppend);
            } else
            {
                throw new System.IO.InternalBufferOverflowException("ArcSight transmission buffer overflow");
            }

        }
        finally
        {
            StringBuilderPool.Free(sb);
            bytePool.Return(buffer);
        }

        return bytesToAppend;


    }

    public int GetSizeOfJSONObject()
    {
        int objectSize = 0;

        objectSize += this.version.ToString().Length + 7 + 6;
        objectSize += this.time.Length + 4 + 6;
        objectSize += this.category.Length + 8 + 6;
        objectSize += this.operationName.Length + 13 + 6;
        objectSize += this.resourceId.Length + 10 + 6;
        objectSize += this.nsgRuleName.Length + 11 + 6;
        objectSize += this.mac.Length + 3 + 6;
        objectSize += this.startTime.Length + 9 + 6;
        objectSize += this.sourceAddress.Length + 13 + 6;
        objectSize += this.destinationAddress.Length + 18 + 6;
        objectSize += this.sourcePort.Length + 10 + 6;
        objectSize += this.destinationPort.Length + 15 + 6;
        objectSize += this.deviceDirection.Length + 15 + 6;
        objectSize += this.deviceAction.Length + 12 + 6;
        if (this.version >= 2.0)
        {
            objectSize += this.flowState.Length + 9 + 6;
            objectSize += this.packetsDtoS == null ? 0 : this.packetsDtoS.Length + 11 + 6;
            objectSize += this.packetsStoD == null ? 0 : this.packetsStoD.Length + 11 + 6;
            objectSize += this.bytesDtoS == null ? 0 : this.bytesDtoS.Length + 9 + 6;
            objectSize += this.bytesStoD == null ? 0 : this.bytesStoD.Length + 9 + 6;
        }
        return objectSize;
    }
}



using System;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Buffers;
using Microsoft.CodeAnalysis.Formatting;

class SplunkEventMessage
{
    public string sourcetype { get; set; }
    public double time { get; set; }
    public DenormalizedRecord @event { get; set; }

    public SplunkEventMessage (DenormalizedRecord splunkEvent)
    {
        sourcetype = "amdl:nsg:flowlogs";
        time = unixTime(splunkEvent.time);
        @event = splunkEvent;
    }

    double unixTime(string time)
    {
        DateTime t = DateTime.ParseExact(time,"yyyy-MM-ddTHH:mm:ss.fffffffZ", System.Globalization.CultureInfo.InvariantCulture);

        double unixTimestamp = t.Ticks - new DateTime(1970, 1, 1).Ticks;
        unixTimestamp /= TimeSpan.TicksPerSecond;
        return unixTimestamp;
    }

    public int GetSizeOfObject()
    {
        return sourcetype.Length + 10 + 6 + (@event == null ? 0 : @event.GetSizeOfJSONObject());
    }
}

class SplunkEventMessages
{
    public List<SplunkEventMessage> splunkEventMessages { get; set; }
}

class DenormalizedRecord
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

class OutgoingRecords
{
    public List<DenormalizedRecord> records { get; set; }
}

class NSGFlowLogTuple
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

class NSGFlowLogsInnerFlows
{
    public string mac { get; set; }
    public string[] flowTuples { get; set; }

    public string MakeMAC()
    {
        var temp = new StringBuilder();
        temp.Append(mac.Substring(0, 2)).Append(":");
        temp.Append(mac.Substring(2, 2)).Append(":");
        temp.Append(mac.Substring(4, 2)).Append(":");
        temp.Append(mac.Substring(6, 2)).Append(":");
        temp.Append(mac.Substring(8, 2)).Append(":");
        temp.Append(mac.Substring(10, 2));

        return temp.ToString();
    }
}

class NSGFlowLogsOuterFlows
{
    public string rule { get; set; }
    public NSGFlowLogsInnerFlows[] flows { get; set; }
}

class NSGFlowLogProperties
{
    public float Version { get; set; }
    public NSGFlowLogsOuterFlows[] flows { get; set; }
}

class NSGFlowLogRecord
{
    public string time { get; set; }
    public string systemId { get; set; }
    public string macAddress { get; set; }
    public string category { get; set; }
    public string resourceId { get; set; }
    public string operationName { get; set; }
    public NSGFlowLogProperties properties { get; set; }

    public string MakeDeviceExternalID()
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

    public string MakeCEFTime()
    {
        // sample input: "2017-08-09T00:13:25.4850000Z"
        // sample output: Aug 09 00:13:25 host CEF:0

        CultureInfo culture = new CultureInfo("en-US");
        DateTime tempDate = Convert.ToDateTime(time, culture);
        string newTime = tempDate.ToString("MMM dd HH:mm:ss");

        return newTime + " host CEF:0";
    }

    public override string ToString()
    {
        string temp = MakeDeviceExternalID();
        return temp;
    }
}

class NSGFlowLogRecords
{
    public NSGFlowLogRecord[] records { get; set; }
}

using System;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using System.Collections.Generic;

class SplunkEventMessage
{
    public string sourcetype { get; set; }
    public DenormalizedRecord @event { get; set; }

    public SplunkEventMessage (DenormalizedRecord splunkEvent)
    {
        sourcetype = "amdl:nsg:flowlogs";
        @event = splunkEvent;
    }

    public int GetSizeOfObject()
    {
        return sourcetype.Length + 10 + 6 + (@event == null ? 0 : @event.GetSizeOfObject());
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

    public override string ToString()
    {
        return JsonConvert.SerializeObject(this, new JsonSerializerSettings
        {
            NullValueHandling = NullValueHandling.Ignore
        });
    }

    public int GetSizeOfObject()
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
                packetsStoD = parts[9];
                bytesStoD = parts[10];
                packetsDtoS = parts[11];
                bytesDtoS = parts[12];
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
                    temp.Append(" bytesIn={0}").Append(bytesStoD);
                    temp.Append(" bytesOut={0}").Append(bytesDtoS);
                }
                else
                {
                    temp.Append(" bytesIn={0}").Append(bytesDtoS);
                    temp.Append(" bytesOut={0}").Append(bytesStoD);
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

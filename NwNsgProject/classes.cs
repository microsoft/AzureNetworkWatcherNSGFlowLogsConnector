using System;
using System.Globalization;
using System.Text.RegularExpressions;
using Newtonsoft.Json;

class NSGFlowLogTuple
{
    string startTime;
    string sourceAddress;
    string destinationAddress;
    string sourcePort;
    string destinationPort;
    string transportProtocol;
    string deviceDirection;
    string deviceAction;

    public NSGFlowLogTuple(string tuple)
    {
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
    }

    public string GetDirection
    {
        get { return deviceDirection; }
    }

    public override string ToString()
    {
        string temp = "";
        temp += "rt=" + (Convert.ToInt32(startTime) * 1000).ToString();
        temp += " src=" + sourceAddress;
        temp += " dst=" + destinationAddress;
        temp += " spt=" + sourcePort;
        temp += " dpt=" + destinationPort;
        temp += " proto=" + (transportProtocol == "U" ? "UDP" : "TCP");
        temp += " deviceDirection=" + (deviceDirection == "I" ? "0" : "1");
        temp += " act=" + deviceAction;

        return temp;
    }
}

class NSGFlowLogsInnerFlows
{
    public string mac { get; set; }
    public string[] flowTuples { get; set; }

    public string MakeMAC()
    {
        string temp = "";
        temp += mac.Substring(0, 2) + ":";
        temp += mac.Substring(2, 2) + ":";
        temp += mac.Substring(4, 2) + ":";
        temp += mac.Substring(6, 2) + ":";
        temp += mac.Substring(8, 2) + ":";
        temp += mac.Substring(10, 2);

        return temp;
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

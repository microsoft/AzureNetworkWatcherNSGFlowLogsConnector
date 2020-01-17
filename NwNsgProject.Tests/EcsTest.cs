using System;
using Xunit;
using Xunit.Abstractions;
using Newtonsoft.Json;
using Microsoft.Extensions.Logging.Abstractions;
using nsgFunc;
using System.IO;
using System.Text;
using System.Reflection;

public class EcsTest
{
    private readonly ITestOutputHelper output;

    public EcsTest(ITestOutputHelper output)
    {
        this.output = output;
    }

    [Fact]
    public void denormalizedRecordJsonTest() {
        DenormalizedRecord denormalizedRecordV2 = EcsTest.createDenormalizedRecordV2();
 
        var outgoingJson = JsonConvert.SerializeObject(denormalizedRecordV2, new JsonSerializerSettings
                {
                    NullValueHandling = NullValueHandling.Ignore
                });
        
        String expected = "{\"time\":\"2020-01-15T07:00:00.5173253Z\",\"category\":\"NetworkSecurityGroupFlowEvent\",\"operationName\":\"NetworkSecurityGroupFlowEvents\",\"resourceId\":\"/SUBSCRIPTIONS/F087A016-314D-482C-93F1-88665DAFBA23/RESOURCEGROUPS/MC_MDRNWRK-DEV-AKS-RESOURCES_MDRNWRK-DEV-AKS_UKSOUTH/PROVIDERS/MICROSOFT.NETWORK/NETWORKSECURITYGROUPS/AKS-AGENTPOOL-14244569-NSG\",\"version\":2.0,\"nsgRuleName\":\"DefaultRule_AllowVnetOutBound\",\"mac\":\"000D3R5F1340\",\"startTime\":\"1578673962\",\"sourceAddress\":\"10.244.0.40\",\"destinationAddress\":\"10.244.1.68\",\"sourcePort\":\"36098\",\"destinationPort\":\"25227\",\"transportProtocol\":\"T\",\"deviceDirection\":\"I\",\"deviceAction\":\"A\",\"flowState\":\"E\",\"packetsStoD\":\"3\",\"bytesStoD\":\"206\",\"packetsDtoS\":\"2\",\"bytesDtoS\":\"140\"}";

        Assert.Equal(expected, outgoingJson);
    }

    [Fact]
    public void denormalizedRecordToEcsTest() {
        DenormalizedRecord denormalizedRecordV2 = EcsTest.createDenormalizedRecordV2();
 
        EcsAll ecsAll = EcsFactory.createEcsAll(denormalizedRecordV2);
        
        var outgoingJson = JsonConvert.SerializeObject(ecsAll, new JsonSerializerSettings
                {
                    NullValueHandling = NullValueHandling.Ignore,
                    Formatting = Newtonsoft.Json.Formatting.Indented,
                });

        output.WriteLine(outgoingJson);
        Assert.Equal(denormalizedRecordV2.time, ecsAll.@timestamp);
        Assert.Equal("AzureNetworkWatcherNSGFlowLogsConnector", ecsAll.agent.name);
        Assert.Equal(denormalizedRecordV2.nsgRuleName, ecsAll.rule.name);
        Assert.Equal("1.0.0", ecsAll.ecs.version);
        Assert.Equal(denormalizedRecordV2.mac, ecsAll.client.mac);
        Assert.Equal(denormalizedRecordV2.category, ecsAll.ecsevent.category);
        Assert.Equal(denormalizedRecordV2.operationName, ecsAll.ecsevent.action);
        Assert.Equal("allowed", ecsAll.ecsevent.outcome);
        Assert.Equal("nsg.access", ecsAll.ecsevent.dataset);
        Assert.Equal(denormalizedRecordV2.resourceId, ecsAll.resource.id);
        Assert.Equal("F087A016-314D-482C-93F1-88665DAFBA23", ecsAll.resource.subscription);
        Assert.Equal("AKS-AGENTPOOL-14244569-NSG", ecsAll.resource.nsg);
        Assert.Equal("10.244.0.40", ecsAll.source.address);
        Assert.Equal("10.244.0.40", ecsAll.source.ip);
        Assert.Equal("10.244.1.68", ecsAll.destination.address);
        Assert.Equal("10.244.1.68", ecsAll.destination.ip);
        Assert.Equal("36098", ecsAll.source.port);
        Assert.Equal("25227", ecsAll.destination.port);       
        Assert.Equal("3", ecsAll.source.packets);
        Assert.Equal("2", ecsAll.destination.packets);       
        Assert.Equal("206", ecsAll.source.bytes);
        Assert.Equal("140", ecsAll.destination.bytes);
        Assert.Equal("tcp", ecsAll.network.transport);
        Assert.Equal("inbound", ecsAll.network.direction);       
        Assert.Equal("transport", ecsAll.network.protocol);
        Assert.Equal("5", ecsAll.network.packets);        
        Assert.Equal("346", ecsAll.network.bytes);       
        Assert.Equal("E", ecsAll.network.flowstate);
    }

    [Fact]
    public void denormalizedRecordToEcsTest2() {
        DenormalizedRecord denormalizedRecordV2 = EcsTest.createDenormalizedRecord2V2();
 
        EcsAll ecsAll = EcsFactory.createEcsAll(denormalizedRecordV2);
        
        var outgoingJson = JsonConvert.SerializeObject(ecsAll, new JsonSerializerSettings
                {
                    NullValueHandling = NullValueHandling.Ignore,
                    Formatting = Newtonsoft.Json.Formatting.Indented,
                });

        output.WriteLine(outgoingJson);
        Assert.Equal(denormalizedRecordV2.time, ecsAll.@timestamp);
        Assert.Equal("AzureNetworkWatcherNSGFlowLogsConnector", ecsAll.agent.name);
        Assert.Equal(denormalizedRecordV2.nsgRuleName, ecsAll.rule.name);
        Assert.Equal("1.0.0", ecsAll.ecs.version);
        Assert.Equal(denormalizedRecordV2.mac, ecsAll.client.mac);
        Assert.Equal(denormalizedRecordV2.category, ecsAll.ecsevent.category);
        Assert.Equal(denormalizedRecordV2.operationName, ecsAll.ecsevent.action);
        Assert.Equal("denied", ecsAll.ecsevent.outcome);
        Assert.Equal("nsg.access", ecsAll.ecsevent.dataset);
        Assert.Equal(denormalizedRecordV2.resourceId, ecsAll.resource.id);
        Assert.Equal("F087A016-314D-482C-93F1-88665DAFBA23", ecsAll.resource.subscription);
        Assert.Equal("AKS-AGENTPOOL-14244569-NSG", ecsAll.resource.nsg);
        Assert.Equal("10.244.0.40", ecsAll.source.address);
        Assert.Equal("10.244.0.40", ecsAll.source.ip);
        Assert.Equal("10.244.1.68", ecsAll.destination.address);
        Assert.Equal("10.244.1.68", ecsAll.destination.ip);
        Assert.Equal("36098", ecsAll.source.port);
        Assert.Equal("25227", ecsAll.destination.port);       
        Assert.Equal("10", ecsAll.source.packets);
        Assert.Equal("9", ecsAll.destination.packets);       
        Assert.Equal("1300", ecsAll.source.bytes);
        Assert.Equal("600", ecsAll.destination.bytes);
        Assert.Equal("udp", ecsAll.network.transport);
        Assert.Equal("outbound", ecsAll.network.direction);       
        Assert.Equal("transport", ecsAll.network.protocol);
        Assert.Equal("19", ecsAll.network.packets);        
        Assert.Equal("1900", ecsAll.network.bytes);       
        Assert.Equal("C", ecsAll.network.flowstate);
    }

   [Fact]
    public void bundleEcsMessageListsJsonTest() {
        NullLogger logger = NullLogger.Instance;

        string workingDirectory = Environment.CurrentDirectory;
        string projectDirectory = Directory.GetParent(workingDirectory).Parent.FullName.Replace("bin", "");
     
        string flowlog = File.ReadAllText(projectDirectory + "AzureNSGFlowLogsv1.json", Encoding.UTF8);
        int count = 0;
        foreach(var bundleOfMessages in Util.bundleEcsMessageListsJson(flowlog, logger))
        {
            output.WriteLine("--------Start of bundleOfMessages----------");
            output.WriteLine(bundleOfMessages);
            output.WriteLine("--------End of bundleOfMessages----------");
            count++;
        }
    
        Assert.Equal(8, count);
    }

    private static DenormalizedRecord createDenormalizedRecordV2()
    {
        NSGFlowLogTuple tuple = new NSGFlowLogTuple("1578673962,10.244.0.40,10.244.1.68,36098,25227,T,I,A,E,3,206,2,140", 2.0f);    

        DenormalizedRecord record = new DenormalizedRecord(2.0f, 
        "2020-01-15T07:00:00.5173253Z",
        "NetworkSecurityGroupFlowEvent",
        "NetworkSecurityGroupFlowEvents",
        "/SUBSCRIPTIONS/F087A016-314D-482C-93F1-88665DAFBA23/RESOURCEGROUPS/MC_MDRNWRK-DEV-AKS-RESOURCES_MDRNWRK-DEV-AKS_UKSOUTH/PROVIDERS/MICROSOFT.NETWORK/NETWORKSECURITYGROUPS/AKS-AGENTPOOL-14244569-NSG",
        "DefaultRule_AllowVnetOutBound",
        "000D3R5F1340",
        tuple);

        return record;
    }

    private static DenormalizedRecord createDenormalizedRecord2V2()
    {
        NSGFlowLogTuple tuple = new NSGFlowLogTuple("1578673962,10.244.0.40,10.244.1.68,36098,25227,U,O,D,C,10,1300,9,600", 2.0f);    

        DenormalizedRecord record = new DenormalizedRecord(2.0f, 
        "2020-01-15T07:00:00.5173253Z",
        "NetworkSecurityGroupFlowEvent",
        "NetworkSecurityGroupFlowEvents",
        "/SUBSCRIPTIONS/F087A016-314D-482C-93F1-88665DAFBA23/RESOURCEGROUPS/MC_MDRNWRK-DEV-AKS-RESOURCES_MDRNWRK-DEV-AKS_UKSOUTH/PROVIDERS/MICROSOFT.NETWORK/NETWORKSECURITYGROUPS/AKS-AGENTPOOL-14244569-NSG",
        "DefaultRule_AllowVnetOutBound",
        "000D3R5F1340",
        tuple);

        return record;
    }


}
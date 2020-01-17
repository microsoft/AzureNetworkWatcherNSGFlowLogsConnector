using Newtonsoft.Json;
public class EcsAll
{
    [JsonProperty(PropertyName = "@timestamp")]
    public string timestamp { get; set; }
    public Agent agent {get; set;}
    public Rule rule {get; set;}
    public Ecs ecs {get; set;}
    public Client client {get; set;}
    [JsonProperty(PropertyName = "event")]
    public EcsEvent ecsevent {get; set;}
    public Resource resource {get; set;}
    public Source source {get; set;}
    public Destination destination {get; set;}
    public Network network {get; set;}
    public EcsAll(DenormalizedRecord denormalizedRecord)
    {
        this.timestamp = denormalizedRecord.time;
        this.agent = new Agent("AzureNetworkWatcherNSGFlowLogsConnector");
        this.rule = new Rule(denormalizedRecord.nsgRuleName);
        this.ecs = new Ecs("1.0.0");
        this.client = new Client(denormalizedRecord.mac);
        this.ecsevent = new EcsEvent(denormalizedRecord);
        this.resource = new Resource(denormalizedRecord.resourceId);
        this.source = new Source(denormalizedRecord);
        this.destination = new Destination(denormalizedRecord);
        this.network = new Network(denormalizedRecord);
    }
}


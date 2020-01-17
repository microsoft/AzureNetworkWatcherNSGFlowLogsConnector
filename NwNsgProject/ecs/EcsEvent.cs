using System;
public class EcsEvent
{
    public string category { get; set; }
    public string action { get; set; }
    public string outcome { get; set; }
    public string start { get; set; }
    public string dataset {get; set;}
    public string ingested {get; set;}
    
    public EcsEvent(DenormalizedRecord denormalizedRecord)
    {
        this.category = denormalizedRecord.category;
        this.action = denormalizedRecord.operationName;

        this.outcome = (denormalizedRecord.deviceAction == "A" ? "allowed" : "denied");

        this.start = denormalizedRecord.startTime;

        this.dataset = "nsg.access";

        DateTime ingestedUtcNow = DateTime.UtcNow;
        this.ingested = ingestedUtcNow.ToString("yyyy-MM-ddThh:mm:ss.fffffffZ");
    }
}
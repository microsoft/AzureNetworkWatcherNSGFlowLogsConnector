using System;
public class BlobName
{
    public string SubscriptionId { get; set; }
    public string ResourceGroupName { get; set; }
    public string NsgName { get; set; }
    public string Year { get; set; }
    public string Month { get; set; }
    public string Day { get; set; }
    public string Hour { get; set; }
    public string Mac { get; set; }

    public BlobName(string subscriptionId, string resourceGroupName, string nsgName, string year, string month, string day, string hour, string mac)
    {
        SubscriptionId = subscriptionId;
        ResourceGroupName = resourceGroupName;
        NsgName = nsgName;
        Year = year;
        Month = month;
        Day = day;
        Hour = hour;
        Mac = mac;
    }

    public BlobName(string subscriptionId, string resourceGroupName, string nsgName, string year, string month, string day, string hour)
    {
        SubscriptionId = subscriptionId;
        ResourceGroupName = resourceGroupName;
        NsgName = nsgName;
        Year = year;
        Month = month;
        Day = day;
        Hour = hour;
        Mac = "none";
    }

    public string GetPartitionKey()
    {
        return string.Format("{0}_{1}_{2}_{3}_{4}_{5}", SubscriptionId.Replace("-", "_"), ResourceGroupName, NsgName, Year, Month, Mac);
    }

    public string GetRowKey()
    {
        return string.Format("{0}_{1}", Day, Hour);
    }
}

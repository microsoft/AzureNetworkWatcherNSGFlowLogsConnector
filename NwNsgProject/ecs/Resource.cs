public class Resource
{
    public string id { get; set; }
    public string subscription {get; set;}
    public string nsg {get; set;}
    public Resource(string id)
    {
        this.id = id;

        char[] sep = new char[] { '/' };
        string[] parts = id.Split(sep);
        this.subscription = parts[2];
        this.nsg = parts[8];
    }
}
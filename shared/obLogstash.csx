#r "System.Net.Http"
#load "getEnvironmentVariable.csx"

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Net;
using System.Net.Http.Headers;

public class SingleHttpClientInstance
{
    private static readonly HttpClient HttpClient;

    static SingleHttpClientInstance()
    {
        HttpClient = new HttpClient();
        HttpClient.Timeout = new TimeSpan(0, 1, 0);
    }

    public static async Task<HttpResponseMessage> SendToLogstash(HttpRequestMessage req)
    {
        HttpResponseMessage response = null;

        try
        {
            response = await HttpClient.SendAsync(req);
        } catch (TaskCanceledException ex)
        {
            throw ex;
        } catch (Exception ex)
        {
            throw ex;
        }
   
        return response;
    }
}

static async Task obLogstash(string newClientContent, TraceWriter log)
{
    string logstashAddress = getEnvironmentVariable("logstashAddress");
    string logstashHttpUser = getEnvironmentVariable("logstashHttpUser");
    string logstashHttpPwd = getEnvironmentVariable("logstashHttpPwd");

    if (logstashAddress.Length == 0 || logstashHttpUser.Length == 0 || logstashHttpPwd.Length == 0)
    {
        log.Error("Values for logstashAddress, logstashHttpUser and logstashHttpPwd are required.");
        return;
    }

    ServicePointManager.Expect100Continue = true;
    ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
    ServicePointManager.ServerCertificateValidationCallback =
    new System.Net.Security.RemoteCertificateValidationCallback(
        delegate { return true; });

    var client = new SingleHttpClientInstance();
    var creds = Convert.ToBase64String(System.Text.ASCIIEncoding.ASCII.GetBytes(string.Format("{0}:{1}", logstashHttpUser, logstashHttpPwd)));
    try
    {
        HttpRequestMessage req = new HttpRequestMessage(HttpMethod.Post, logstashAddress);
        req.Headers.Accept.Clear();
        req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        req.Headers.Add("Authorization", "Basic " + creds);
        req.Content = new StringContent(newClientContent, Encoding.UTF8, "application/json");
        HttpResponseMessage response = await SingleHttpClientInstance.SendToLogstash(req);
        if (response.StatusCode != HttpStatusCode.OK)
        {
            log.Error($"StatusCode from Logstash: {response.StatusCode}, and reason: {response.ReasonPhrase}");
        }
    }
    catch (System.Net.Http.HttpRequestException e)
    {
        string msg = e.Message;
        if (e.InnerException != null)
        {
            msg += " *** " + e.InnerException.Message;
        }
        log.Error($"HttpRequestException Error: \"{msg}\" was caught while sending to Logstash.");
        throw e;
    }
    catch (Exception f)
    {
        string msg = f.Message;
        if (f.InnerException != null)
        {
            msg += " *** " + f.InnerException.Message;
        }
        log.Error($"Unknown error caught while sending to Logstash: \"{f.ToString()}\"");
        throw f;
    }
}
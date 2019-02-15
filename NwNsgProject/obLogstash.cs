using Microsoft.Extensions.Logging;
using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace nsgFunc
{
    public partial class Util
    {
        public static async Task obLogstash(string newClientContent, ILogger log)
        {
            string logstashAddress = Util.GetEnvironmentVariable("logstashAddress");
            string logstashHttpUser = Util.GetEnvironmentVariable("logstashHttpUser");
            string logstashHttpPwd = Util.GetEnvironmentVariable("logstashHttpPwd");

            if (logstashAddress.Length == 0 || logstashHttpUser.Length == 0 || logstashHttpPwd.Length == 0)
            {
                log.LogError("Values for logstashAddress, logstashHttpUser and logstashHttpPwd are required.");
                return;
            }

            ServicePointManager.Expect100Continue = true;
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            ServicePointManager.ServerCertificateValidationCallback =
            new System.Net.Security.RemoteCertificateValidationCallback(
                delegate { return true; });

            // log.Info($"newClientContent: {newClientContent}");

            var client = new Util.SingleHttpClientInstance();
            var creds = Convert.ToBase64String(System.Text.ASCIIEncoding.ASCII.GetBytes(string.Format("{0}:{1}", logstashHttpUser, logstashHttpPwd)));
            try
            {
                HttpRequestMessage req = new HttpRequestMessage(HttpMethod.Post, logstashAddress);
                req.Headers.Accept.Clear();
                req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                req.Headers.Add("Authorization", "Basic " + creds);
                req.Content = new StringContent(newClientContent, Encoding.UTF8, "application/json");
                HttpResponseMessage response = await Util.SingleHttpClientInstance.SendToLogstash(req, log);
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    log.LogError($"StatusCode from Logstash: {response.StatusCode}, and reason: {response.ReasonPhrase}");
                }
            }
            catch (System.Net.Http.HttpRequestException e)
            {
                string msg = e.Message;
                if (e.InnerException != null)
                {
                    msg += " *** " + e.InnerException.Message;
                }
                log.LogError($"HttpRequestException Error: \"{msg}\" was caught while sending to Logstash.");
                throw e;
            }
            catch (Exception f)
            {
                string msg = f.Message;
                if (f.InnerException != null)
                {
                    msg += " *** " + f.InnerException.Message;
                }
                log.LogError($"Unknown error caught while sending to Logstash: \"{f.ToString()}\"");
                throw f;
            }
        }

    }
}

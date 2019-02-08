using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace NwNsgProject
{
    public partial class Util
    {
        public static string GetEnvironmentVariable(string name)
        {
            var result = System.Environment.GetEnvironmentVariable(name, System.EnvironmentVariableTarget.Process);
            if (result == null)
                return "";

            return result;
        }

        public class SingleHttpClientInstance
        {
            private static readonly HttpClient HttpClient;

            static SingleHttpClientInstance()
            {
                HttpClient = new HttpClient();
                HttpClient.Timeout = new TimeSpan(0, 1, 0);
            }

            public static async Task<HttpResponseMessage> SendToLogstash(HttpRequestMessage req, TraceWriter log)
            {
                HttpResponseMessage response = null;
                var httpClient = new HttpClient();
                httpClient.Timeout = TimeSpan.FromMinutes(5);
                try
                {
                    response = await httpClient.SendAsync(req);
                }
                catch (AggregateException ex)
                {
                    log.Error("Got AggregateException.");
                    throw ex;
                }
                catch (TaskCanceledException ex)
                {
                    log.Error("Got TaskCanceledException.");
                    throw ex;
                }
                catch (Exception ex)
                {
                    log.Error("Got other exception.");
                    throw ex;
                }
                return response;
            }

            public static async Task<HttpResponseMessage> SendToSplunk(HttpRequestMessage req)
            {
                HttpResponseMessage response = await HttpClient.SendAsync(req);
                return response;
            }

        }

        public static async Task logErrorRecord(string errorRecord, Binder errorRecordBinder, TraceWriter log)
        {
            if (errorRecordBinder == null) { return; }

            Byte[] transmission = new Byte[] { };

            try
            {
                transmission = AppendToTransmission(transmission, errorRecord);

                Guid guid = Guid.NewGuid();
                var attributes = new Attribute[]
                {
                    new BlobAttribute(String.Format("errorrecord/{0}", guid)),
                    new StorageAccountAttribute("cefLogAccount")
                };

                CloudBlockBlob blob = await errorRecordBinder.BindAsync<CloudBlockBlob>(attributes);
                blob.UploadFromByteArray(transmission, 0, transmission.Length);

                transmission = new Byte[] { };
            }
            catch (Exception ex)
            {
                log.Error($"Exception logging record: {ex.Message}");
            }
        }

        static async Task logErrorRecord(NSGFlowLogRecord errorRecord, Binder errorRecordBinder, TraceWriter log)
        {
            if (errorRecordBinder == null) { return; }

            Byte[] transmission = new Byte[] { };

            try
            {
                transmission = Util.AppendToTransmission(transmission, errorRecord.ToString());

                Guid guid = Guid.NewGuid();
                var attributes = new Attribute[]
                {
                    new BlobAttribute(String.Format("errorrecord/{0}", guid)),
                    new StorageAccountAttribute("cefLogAccount")
                };

                CloudBlockBlob blob = await errorRecordBinder.BindAsync<CloudBlockBlob>(attributes);
                blob.UploadFromByteArray(transmission, 0, transmission.Length);

                transmission = new Byte[] { };
            }
            catch (Exception ex)
            {
                log.Error($"Exception logging record: {ex.Message}");
            }
        }

        public static Byte[] AppendToTransmission(Byte[] existingMessages, string appendMessage)
        {
            Byte[] appendMessageBytes = Encoding.ASCII.GetBytes(appendMessage);
            Byte[] crlf = new Byte[] { 0x0D, 0x0A };

            Byte[] newMessages = new Byte[existingMessages.Length + appendMessage.Length + 2];

            existingMessages.CopyTo(newMessages, 0);
            appendMessageBytes.CopyTo(newMessages, existingMessages.Length);
            crlf.CopyTo(newMessages, existingMessages.Length + appendMessageBytes.Length);

            return newMessages;
        }

        // typical use cases
        // , key: value ==> , "key": "value" --> if there's a comma, there's a colon
        // key: value ==> "key": "value" --> if there's no comma, there may be a colon
        static string eqs(string inString)
        {
            // eqs = Escape Quote String

            return "\"" + inString + "\"";
        }

        static string eqs(bool prependComma, string inString, bool appendColon)
        {
            var outString = String.Concat((prependComma ? "," : ""), eqs(inString), (appendColon ? ":" : ""));

            return outString;
        }

        static string eqs(string inString, bool appendColon)
        {
            return eqs(false, inString, appendColon);
        }

        static string eqs(bool prependComma, string inString)
        {
            return eqs(prependComma, inString, true);
        }

        static string kvp(string key, string value)
        {
            return eqs(true, key) + eqs(value);
        }

        static string kvp(bool firstOne, string key, string value)
        {
            return eqs(key, true) + eqs(value);
        }


    }
}

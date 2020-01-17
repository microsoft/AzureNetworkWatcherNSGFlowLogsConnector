using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Collections.Generic;

namespace nsgFunc
{
    public partial class Util
    {
        private static Lazy<EventHubClient> LazyEventHubConnection = new Lazy<EventHubClient>(() =>
        {
            string EventHubConnectionString = GetEnvironmentVariable("eventHubConnection");
            string EventHubName = GetEnvironmentVariable("eventHubName");

            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EventHubConnectionString)
            {
                EntityPath = EventHubName
            };
            var eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            return eventHubClient;
        });

        public static async Task<int> obEventHub(string newClientContent, ILogger log)
        {
            var eventHubClient = LazyEventHubConnection.Value;
            int bytesSent = 0;

            foreach (var bundleOfMessages in bundleEcsMessageListsJson(newClientContent, log))
            {
                await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(bundleOfMessages)));
                bytesSent += bundleOfMessages.Length;
            }
            return bytesSent;
        }

        static System.Collections.Generic.IEnumerable<string> bundleMessageListsJson(string newClientContent, ILogger log)
        {
            foreach (var messageList in denormalizedRecords(newClientContent, null, log))
            {
                var outgoingRecords = new OutgoingRecords();
                outgoingRecords.records = messageList;

                var outgoingJson = JsonConvert.SerializeObject(outgoingRecords, new JsonSerializerSettings
                {
                    NullValueHandling = NullValueHandling.Ignore
                });

                yield return outgoingJson;
            }
        }

        public static System.Collections.Generic.IEnumerable<string> bundleEcsMessageListsJson(string newClientContent, ILogger log)
        {
            foreach (List<DenormalizedRecord> messageList in denormalizedRecords(newClientContent, null, log))
            {
                foreach(DenormalizedRecord denormalizedRecord in messageList)
                {
                    OutgoingEcsRecord ecsRecord = new OutgoingEcsRecord();
                    ecsRecord.message = new EcsAll(denormalizedRecord);
                    var outgoingEcsJson = JsonConvert.SerializeObject(ecsRecord, new JsonSerializerSettings
                    {
                        NullValueHandling = NullValueHandling.Ignore
                    });

                    yield return outgoingEcsJson;
                }
            }
        }
    }
}

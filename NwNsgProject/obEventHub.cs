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
        const int MAXTRANSMISSIONSIZE = 512 * 1024;
        //const int MAXTRANSMISSIONSIZE = 2 * 1024;

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

            foreach (var bundleOfMessages in bundleMessageListsJson(newClientContent, log))
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
                //
                // messageList looks like this: (List<xxx>)
                //
                // {
                //   "time": "xxx",
                //   "category": "xxx",
                //   "operationName": "xxx",
                //   "version": "xxx",
                //   "deviceExtId": "xxx",
                //   "flowOrder": "xxx",
                //   "nsgRuleName": "xxx",
                //   "dmac|smac": "xxx",
                //   "rt": "xxx",
                //   "src": "xxx",
                //   "dst": "xxx",
                //   "spt": "xxx",
                //   "dpt": "xxx",
                //   "proto": "xxx",
                //   "deviceDirection": "xxx",
                //   "act": "xxx"
                //  }

                var outgoingRecords = new OutgoingRecords();
                outgoingRecords.records = messageList;

                var outgoingJson = JsonConvert.SerializeObject(outgoingRecords, new JsonSerializerSettings
                {
                    NullValueHandling = NullValueHandling.Ignore
                });

                yield return outgoingJson;
            }
        }

        static IEnumerable<List<DenormalizedRecord>> denormalizedRecords(string newClientContent, Binder errorRecordBinder, ILogger log)
        {
            var outgoingList = new List<DenormalizedRecord>(450);
            var sizeOfListItems = 0;

            NSGFlowLogRecords logs = JsonConvert.DeserializeObject<NSGFlowLogRecords>(newClientContent);

            foreach (var record in logs.records)
            {
                float version = record.properties.Version;

                foreach (var outerFlow in record.properties.flows)
                {
                    foreach (var innerFlow in outerFlow.flows)
                    {
                        foreach (var flowTuple in innerFlow.flowTuples)
                        {
                            var tuple = new NSGFlowLogTuple(flowTuple, version);

                            var denormalizedRecord = new DenormalizedRecord(
                                record.properties.Version,
                                record.time,
                                record.category,
                                record.operationName,
                                record.resourceId,
                                outerFlow.rule,
                                innerFlow.mac,
                                tuple);

                            var sizeOfDenormalizedRecord = denormalizedRecord.SizeOfObject();

                            if (sizeOfListItems + sizeOfDenormalizedRecord > MAXTRANSMISSIONSIZE + 20)
                            {
                                yield return outgoingList;
                                outgoingList.Clear();
                                sizeOfListItems = 0;
                            }
                            outgoingList.Add(denormalizedRecord);
                            sizeOfListItems += sizeOfDenormalizedRecord;
                        }
                    }
                }
            }
            if (sizeOfListItems > 0)
            {
                yield return outgoingList;
            }
        }
    }
}

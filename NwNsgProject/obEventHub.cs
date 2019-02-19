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

        public static async Task obEventHub(string newClientContent, ILogger log)
        {
            var eventHubClient = LazyEventHubConnection.Value;

            foreach (var bundleOfMessages in bundleMessageListsJson(newClientContent, log))
            {
                await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(bundleOfMessages)));
            }
        }

        static System.Collections.Generic.IEnumerable<string> bundleMessages(string newClientContent, ILogger log)
        {
            var numberOfItemsInBundle = 0;
            var transmission = new StringBuilder(MAXTRANSMISSIONSIZE);
            transmission.Append("{\"records\":[");
            bool firstRecord = true;
            foreach (var message in denormalizeRecords(newClientContent, null, log))
            {
                //
                // message looks like this:
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

                if (transmission.Length + message.Length > MAXTRANSMISSIONSIZE)
                {
                    transmission.Append("]}");
                    log.LogInformation("Number of items in bundle: {0}", numberOfItemsInBundle);
                    numberOfItemsInBundle = 0;
                    yield return transmission.ToString();
                    transmission.Clear();
                    transmission.Append("{\"records\":[");
                    firstRecord = true;
                }

                // add comma after existing transmission if it's not the first record
                if (firstRecord)
                {
                    firstRecord = false;
                }
                else
                {
                    transmission.Append(",");
                }

                transmission.Append(message);
                numberOfItemsInBundle++;
            }
            if (transmission.Length > 0)
            {
                transmission.Append("]}");
                yield return transmission.ToString();
            }
        }

        static System.Collections.Generic.IEnumerable<string> bundleMessageLists(string newClientContent, ILogger log)
        {
            var transmission = new StringBuilder(MAXTRANSMISSIONSIZE);
            foreach (var messageList in denormalizeLists(newClientContent, null, log))
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
                transmission.Append("{\"records\":[");
                var numberOfItemsInBundle = 0;
                bool firstRecord = true;

                foreach (var message in messageList)
                {
                    if (firstRecord)
                    {
                        firstRecord = false;
                    }
                    else
                    {
                        transmission.Append(",");
                    }

                    transmission.Append(message);
                    numberOfItemsInBundle++;
                }
                transmission.Append("]}");
                log.LogInformation("Number of items in bundle: {0}", numberOfItemsInBundle);
                numberOfItemsInBundle = 0;
                yield return transmission.ToString();
                transmission.Clear();
            }
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

        static System.Collections.Generic.IEnumerable<string> denormalizeRecords(string newClientContent, Binder errorRecordBinder, ILogger log)
        {
            //
            // newClientContent looks like this:
            //
            // {
            //   "records":[
            //     {...},
            //     {...}
            //     ...
            //   ]
            // }
            //

            NSGFlowLogRecords logs = JsonConvert.DeserializeObject<NSGFlowLogRecords>(newClientContent);

            string logIncomingJSON = Util.GetEnvironmentVariable("logIncomingJSON");
            Boolean flag;
            if (Boolean.TryParse(logIncomingJSON, out flag))
            {
                if (flag)
                {
                    logErrorRecord(newClientContent, errorRecordBinder, log).Wait();
                }
            }

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

                            var denormalizedObject = new DenormalizedRecord(
                                record.properties.Version,
                                record.time,
                                record.category,
                                record.operationName,
                                record.resourceId,
                                outerFlow.rule,
                                innerFlow.mac,
                                tuple);
                            string outgoingJson = denormalizedObject.ToString();

                            yield return outgoingJson;
                        }
                    }
                }
            }

        }
        static System.Collections.Generic.IEnumerable<List<string>> denormalizeLists(string newClientContent, Binder errorRecordBinder, ILogger log)
        {
            var outgoingList = new List<string>(450);
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

                            var denormalizedObject = new DenormalizedRecord(
                                record.properties.Version,
                                record.time,
                                record.category,
                                record.operationName,
                                record.resourceId,
                                outerFlow.rule,
                                innerFlow.mac,
                                tuple);
                            string outgoingJson = denormalizedObject.ToString();

                            if (sizeOfListItems + outgoingJson.Length > MAXTRANSMISSIONSIZE+20)
                            {
                                yield return outgoingList;
                                outgoingList.Clear();
                                sizeOfListItems = 0;
                            }
                            outgoingList.Add(outgoingJson);
                            sizeOfListItems += outgoingJson.Length;
                        }
                    }
                }
            }
            if (outgoingList.Count > 0)
            {
                yield return outgoingList;
            }
        }

        static IEnumerable<DenormalizedRecord[]> denormalizeArrays(string newClientContent, Binder errorRecordBinder, ILogger log)
        {
            var outgoingArray = Array.CreateInstance(typeof(DenormalizedRecord), 450);
            var sizeOfArrayItems = 0;
            var arrayItemIndex = 0;
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

                            var denormalizedObject = new DenormalizedRecord(
                                record.properties.Version,
                                record.time,
                                record.category,
                                record.operationName,
                                record.resourceId,
                                outerFlow.rule,
                                innerFlow.mac,
                                tuple);
                            string outgoingJson = denormalizedObject.ToString();

                            if (sizeOfArrayItems + outgoingJson.Length > MAXTRANSMISSIONSIZE + 20)
                            {
                                yield return (DenormalizedRecord[])outgoingArray;
                                Array.Clear(outgoingArray, 0, 450);
                                sizeOfArrayItems = 0;
                                arrayItemIndex = 0;
                            }
                            outgoingArray.SetValue(denormalizedObject, arrayItemIndex++);
                            sizeOfArrayItems += outgoingJson.Length;
                        }
                    }
                }
            }
            if (arrayItemIndex > 0)
            {
                yield return (DenormalizedRecord[])outgoingArray;
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

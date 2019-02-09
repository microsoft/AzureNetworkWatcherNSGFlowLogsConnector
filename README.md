This project installs into an Azure Function in your Azure subscription. Its job is to read NSG Flow Logs from your configured storage account, break the data into chunks that are the right size for your log analytics system to ingest, then transmit the chunks to that system. At present, you may choose from four output bindings: ArcSight, LogStash, Splunk HEC, Event Hub.  


[![Deploy to Azure](http://azuredeploy.net/deploybutton.png)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2Fsebastus%2FAzureFunctionDeployment%2FNwNSGFlowLogs%2FazureDeploy.json)


NOTE regarding the Event Hub output binding:  

Native support for event hubs is not yet available, but would be the preferred method. If you use Splunk and prefer to send NSG flow logs to Splunk using event hub rather than HEC, the event hub output binding will do the job. In [Azure Monitor Addon For Splunk](https://github.com/Microsoft/AzureMonitorAddonForSplunk), configure the Azure Monitor Diagnostic Logs data input and add a line to ```TA-folder/bin/app/hubs.json``` similar to this:  

Example: ```'insights-logs-nsgflowlogs': 'resourceId'```  

When you create the hub (e.g. ```insights-logs-nsgflowlogs```) set the number of partitions to 4. This is mandatory.

# Settings

In the Application Settings of your Azure Function:
* AppName                     - this is the name of the function app. In the Azure Portal, this is the name that will appear in the list of resources.  
   Example: ```MyNSGApp```  
* appServicePlan              - "ServicePlan" or "Consumption".  
   If you select "ServicePlan", an App Service Plan will be created and you will be billed accordingly. If you select "Consumption", you will be billed based on the Consumption plan.  
* appServicePlanTier          - "Free", "Shared", "Basic", "Standard", "Premium", "PremiumV2"  
   Example: ```Standard```  
   (only relevant for ServicePlan)  
* appServicePlanName          - depends on tier, for full details see "Choose your pricing tier" in the portal on an App service plan "Scale up" applet.  
   Example: For standard tier, "S1", "S2", "S3" are options for plan name  
   (only relevant for ServicePlan)  
* appServicePlanCapacity      - how many instances do you want to set for the upper limit?  
   Example: For standard tier, S2, set a value from 1 to 10  
   (only relevant for ServicePlan)  
* githubRepoURL                     - this is the URL of the repo that contains the function app source. You would put your fork's address here.  
   Example: ```https://github.com/microsoft/AzureNetworkWatcherNSGFlowLogsConnector```  
* githubRepoBranch                  - this is the name of the branch containing the code you want to deploy.  
   Example: ```master```  
* nsgSourceDataConnection     - a storage account connection string  
   Example: ```DefaultEndpointsProtocol=https;AccountName=yyy;AccountKey=xxx;EndpointSuffix=core.windows.net```  
* cefLogAccount               - a storage account connection string - account into which trace logs of incoming json and outgoing cef are dropped  
   Example: ```DefaultEndpointsProtocol=https;AccountName=yyy;AccountKey=xxx;EndpointSuffix=core.windows.net```  
* outputBinding               - Points to the destination service - the service that will receive the NSG flow log data. Options are "arcsight", "splunk", "eventhub", "logstash".  
   Example: ```arcsight```  
* arcsightAddress             - internet address of the ArcSight server / service  
   Example: ```192.168.1.1```  
* arcsightPort                - TCP port to connect to on destination server / service  
   Example: ```1514```
* splunkAddress               - internet address of the Splunk HEC port.  
   Example: ```http://mysplunkbox.uksouth.cloudapp.azure.com:8088/services/collector```  
* splunkToken                 - guid security token for Splunk HEC  
   Example: ```a77fdc21-0861-4d8b-941c-e1b4c556b4fb```
* eventHubConnection          - connection string for your event hub namespace  
   Example: ```Endpoint=sb://my.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=key```
* eventHubName                - name of your event hub within the hub namespace  
   Example: ```insights-logs-nsgflowlogs```  
* logstashAddress             - network address of LogStash input endpoint  
   Example: ```http://myelasticbox.uksouth.cloudapp.azure.com/```  
* logstashHttpUser            - userid for LogStash http input  
   Example: ```greg```  
* logstashHttpPwd             - password for LogStash http input  
   Example: ```P@ssw0rd!```  


# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

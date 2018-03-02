This project installs into an Azure Function in your Azure subscription. Its job is to read NSG Flow Logs from your configured storage account, break the data into chunks that are the right size for your log analytics system to ingest, then transmit the chunks to that system.

[![Deploy to Azure](http://azuredeploy.net/deploybutton.png)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2Fsebastus%2FAzureFunctionDeployment%2FNwNSGFlowLogs%2FazureDeploy.json)

# Settings

In the Application Settings of your Azure Function:
* AzureWebJobsStorage - required by all Azure Functions
* AzureWebJobsDashboard - required by all Azure Functions
* nsgSourceDataConnection - an Azure storage account connection string, the account where NSG flow logs land
* outputBinding - 'arcsight' (there may be other options in future)
* arcsightAddress - an IP address or DNS name for your ArcSight server/service
* arcsightPort - TCPIP port, usually 1514
* cefLogAccount - an Azure storage account connection string. Needed only if you want to log incoming JSON files and outgoing CEF files.
* logIncomingJSON - true/false
* logOutgoingCEF - true/false

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

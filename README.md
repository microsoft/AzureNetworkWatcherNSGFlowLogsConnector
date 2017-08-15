This project installs into an Azure Function in your Azure subscription. Its job is to read NSG Flow Logs from your configured storage account, break the data into chunks that are the right size for your log analytics system to ingest, then transmit the chunks to that system.

# Settings

In the Application Settings of your Azure Function:
* AzureWebJobsStorage - required by all Azure Functions
* AzureWebJobsDashboard - required by all Azure Functions
* nsgtelemetry - the connection string of the storage account that receives your NSG flow logs
* nsgSourceDataAccount - points to the connection string, so in the simplest case, "nsgtelemetry" is what you need to enter
* blobContainerName - usually "insights-logs-networksecuritygroupflowevent"
* logstashAddress - something like "http://mylogstashurl:8080"
* logstashHttpUser - Http username configured in your Logstash config
* logstashHttpPwd - Http password configured in your Logstash config

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

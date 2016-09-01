# eventhubs-sample-event-producer
Example showing how events can be generated and pushed to Microsoft Azure Sevicebus Eventhubs

Usage: java -cp com-microsoft-eventhubs-client-example-0.2.0.jar com.microsoft.eventhubs.client.example.EventhubsClientDriver --eventhubs-namespace "eventhubsNamespace" --eventhubs-name "eventhubsName" --policy-name "eventhubsPolicyName" --policy-key "eventhubsPolicyKey" --message-length messageLength --thread-count threadCount --message-count messageCount|-1 (-1 for continue without stopping)

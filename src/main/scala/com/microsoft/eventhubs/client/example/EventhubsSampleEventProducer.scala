package com.microsoft.eventhubs.client.example

import java.util.Calendar
import com.microsoft.eventhubs.client.{EventHubSender, EventHubClient}

import scala.util.Random

class EventhubsSampleEventProducer(
                                    val policyName: String,
                                    val policyKey: String,
                                    val eventHubsNamespace: String,
                                    val eventHubsName: String,
                                    val eventLength: Int,
                                    val eventStartIndex: Long,
                                    val eventCount: Long,
                                    val partitionId: String) {

  def GenerateEvents(): Long = {

    val randomGenerator: Random = new Random()

    val eventHubsClient: EventHubClient = EventHubClient.create(policyName, policyKey, eventHubsNamespace, eventHubsName)
    val eventHubsSender: EventHubSender = eventHubsClient.createPartitionSender(partitionId)

    var currentEventCount: Long = 0
    var eventIndex: Long = eventStartIndex

    while(true) {

      val currentTime = Calendar.getInstance().getTime()

      try {

        val randomString: String = randomGenerator.alphanumeric.take(eventLength).mkString

        eventHubsSender.send(randomString)

        if(currentEventCount % 1024 == 0) {

          eventIndex = currentEventCount + eventStartIndex

          val threadId = Thread.currentThread().getId()

          println(s"$threadId > $eventIndex > $currentTime > Sending event: $randomString")
        }

        currentEventCount += 1

        if (eventCount > 0) {

          if (currentEventCount >= eventCount) {

            return currentEventCount
          }
        }
      }
      catch {

        case e: Exception => s"$eventIndex > $currentTime > Exception: $e"
      }
    }

    return currentEventCount
  }
}
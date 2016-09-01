/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.eventhubs.client.example

import java.util.Calendar

import com.microsoft.azure.eventhubs.{EventData, EventHubClient}
import com.microsoft.azure.servicebus.ConnectionStringBuilder
//import org.json4s._
//import org.json4s.jackson.Serialization._

import scala.util.Random

//case class EventPayload(payloadBody: String)

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

    val connectionString: ConnectionStringBuilder = new ConnectionStringBuilder(eventHubsNamespace, eventHubsName,
      policyName, policyKey)

    val eventHubsClient: EventHubClient = EventHubClient.createFromConnectionString(connectionString.toString).get

    val randomGenerator: Random = new Random()

    var currentEventCount: Long = 0
    var eventIndex: Long = eventStartIndex

    while(true) {

      val currentTime = Calendar.getInstance().getTime

      try {

        //val eventPayload: EventPayload = EventPayload(randomGenerator.alphanumeric.take(eventLength).mkString)

        //implicit val formats = DefaultFormats

        val eventPayload: String = randomGenerator.alphanumeric.take(eventLength).mkString

        val eventData: EventData = new EventData(eventPayload.getBytes())

        eventHubsClient.send(eventData)

        if(currentEventCount % 1024 == 0) {

          eventIndex = currentEventCount + eventStartIndex

          val threadId = Thread.currentThread().getId

          println(s"$threadId > $eventIndex > $currentTime > Sending event: $eventPayload")
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

    currentEventCount
  }
}
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

package com.microsoft.azure.eventhubs.client.example

import java.text.SimpleDateFormat
import java.util.Calendar

import com.microsoft.azure.eventhubs.{EventData, EventHubClient}
import com.microsoft.azure.servicebus.ConnectionStringBuilder
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

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
    var currentEventIndex: Long = eventStartIndex

    val threadId = Thread.currentThread().getId
    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'")

    while(true) {

      val currentTime = Calendar.getInstance().getTime

      try {

        val eventPayload: String = (1 to eventLength).map((_) => compact(render(getEventJson(df, randomGenerator)))).mkString("")

        val eventData: EventData = new EventData(eventPayload.getBytes())

        eventHubsClient.sendSync(eventData)

        currentEventCount += 1

        if(currentEventCount % 1024 == 0) {

          currentEventIndex = currentEventCount + eventStartIndex

          println(s"$threadId > $currentTime > $currentEventCount > $currentEventIndex > Sending event: $eventPayload")
        }

        if (eventCount > 0 && currentEventCount >= eventCount) return currentEventCount
      }
      catch {

        case e: Exception => println(s"$currentEventIndex > $currentTime > Exception: $e")
      }
    }

    currentEventCount
  }

  private def getEventJson(df: SimpleDateFormat, randomGenerator: Random) = {
    ("Time" -> df.format(Calendar.getInstance().getTime)) ~
      ("Server" -> randomGenerator.alphanumeric.take(12).toString()) ~
      ("Region" -> "NAM") ~
      ("SpamEngineRolloutStage" -> (500 + randomGenerator.nextInt(150)).toString) ~
      ("Forest" -> s"namprd${randomGenerator.nextInt(10)}.prod.outlook.com") ~
      ("Dag" -> s"nampr${randomGenerator.nextInt(10)}dg${randomGenerator.nextInt(1000)}") ~
      ("Datacenter" -> s"DM${randomGenerator.nextInt(10)}") ~
      ("ExchangeVersion" ->  s"15.20.0178.${randomGenerator.nextInt(100)}") ~
      ("KeySource" -> s"SpamFilter${randomGenerator.nextInt(20)}") ~
      ("KeyEntity" -> s"PartitionId${randomGenerator.nextInt(50)}") ~
      ("KeyEvent" -> s"IPSpam${randomGenerator.nextInt(50)}") ~
      ("Value" -> s"${randomGenerator.nextInt()}") ~
      ("Type" -> "OpticsLogSinkCount")
  }
}
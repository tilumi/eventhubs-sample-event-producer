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

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

object EventhubsClientDriver {

  def main(inputArguments: Array[String]): Unit = {

    val inputOptions = ClientArgumentParser.parseArguments(Map(), inputArguments.toList)

    ClientArgumentParser.verifyArguments(inputOptions)

    println(inputOptions)

    var partitionId: String = null
    var threadCount: Int = 1
    var messageCount: Long = -1

    if(inputOptions.contains(Symbol(ClientArgumentKeys.PartitionID))) {

      partitionId = inputOptions(Symbol(ClientArgumentKeys.PartitionID)).asInstanceOf[String]
    }

    if(inputOptions.contains(Symbol(ClientArgumentKeys.MessageCount))) {

      messageCount = inputOptions(Symbol(ClientArgumentKeys.MessageCount)).asInstanceOf[Long]
    }

    if(inputOptions.contains(Symbol(ClientArgumentKeys.ThreadCount))) {

      threadCount = inputOptions(Symbol(ClientArgumentKeys.ThreadCount)).asInstanceOf[Int]
    }

    var messageCountPerThread: Long = 0

    if (messageCount > 0) {

      messageCountPerThread = Math.ceil(messageCount/threadCount).toLong
    }

    val producerTasks = for (i <- 0 to threadCount - 1) yield Future {

      val eventProducer: EventhubsSampleEventProducer = new EventhubsSampleEventProducer(
        inputOptions(Symbol(ClientArgumentKeys.PolicyName)).asInstanceOf[String],
        inputOptions(Symbol(ClientArgumentKeys.PolicyKey)).asInstanceOf[String],
        inputOptions(Symbol(ClientArgumentKeys.EventhubsNamespace)).asInstanceOf[String],
        inputOptions(Symbol(ClientArgumentKeys.EventhubsName)).asInstanceOf[String],
        inputOptions(Symbol(ClientArgumentKeys.MessageLength)).asInstanceOf[Int],
        i * messageCountPerThread,
        messageCountPerThread,
        partitionId)

      eventProducer.GenerateEvents()
    }

    val eventFutures: Future[Long] = Future.reduce(producerTasks)((x, y) => x + y)

    val totalEvents = Await.result(eventFutures, Duration.Inf)

    println(s"Total Events Sent: $totalEvents")
  }
}

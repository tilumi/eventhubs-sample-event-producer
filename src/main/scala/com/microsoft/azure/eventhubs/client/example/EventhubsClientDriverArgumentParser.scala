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

object ClientArgumentKeys extends Enumeration {
  val EventhubsNamespace: String = "eventhubsNamespace"
  val EventhubsName: String = "eventhubsName"
  val PolicyName: String = "policyName"
  val PolicyKey: String = "policyKey"
  val MessageLength: String = "messageLength"
  val MessageCount: String = "messageCount"
  val ThreadCount: String = "threadCount"
  val PartitionID: String = "partitionID"
}

object ClientArgumentParser {

  type ArgumentMap = Map[Symbol, Any]

  def usageExample(): Unit = {

    val eventhubsNamespace: String = "sparkstreamingeventhub-ns"
    val eventhubsName: String = "sparkstreamingeventhub"
    val policyName: String = "eventlistenpolicy"
    val policyKey: String = "0CrxnL103K4kHkM+HyxZJAA/3qV5ycrvM2YzGckpBUI="
    val messageLength: Int = 32
    val messageCount: Long = -1
    val threadCount: Int = 16
    val partitionID: Int = -1

    println()
    println(s"Usage: --eventhubs-namespace $eventhubsNamespace --eventhubs-name $eventhubsName" +
      s" --policy-name $policyName --policy-key $policyKey" +
      s" --message-length  $messageLength --message-count $messageCount" +
      s" --thread-count $threadCount --partition-id $partitionID")
    println()
  }

  def parseArguments(argumentMap : ArgumentMap, argumentList: List[String]) : ArgumentMap = {

    argumentList match {
      case Nil => argumentMap
      case "--eventhubs-namespace" :: value:: tail =>
        parseArguments(argumentMap ++ Map(Symbol(ClientArgumentKeys.EventhubsNamespace) -> value.toString), tail)
      case "--eventhubs-name" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(ClientArgumentKeys.EventhubsName) -> value.toString), tail)
      case "--policy-name" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(ClientArgumentKeys.PolicyName) -> value.toString), tail)
      case "--policy-key" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(ClientArgumentKeys.PolicyKey) -> value.toString), tail)
      case "--message-length" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(ClientArgumentKeys.MessageLength) -> value.toInt), tail)
      case "--message-count" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(ClientArgumentKeys.MessageCount) -> value.toLong), tail)
      case "--thread-count" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(ClientArgumentKeys.ThreadCount) -> value.toInt), tail)
      case "--partition-id" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(ClientArgumentKeys.PartitionID) -> value.toInt), tail)
      case option :: tail =>
        println()
        println("Unknown option: " + option)
        println()
        usageExample()
        sys.exit(1)
    }
  }

  def verifyArguments(argumentMap : ArgumentMap): Unit = {

    assert(argumentMap.contains(Symbol(ClientArgumentKeys.EventhubsNamespace)))
    assert(argumentMap.contains(Symbol(ClientArgumentKeys.EventhubsName)))
    assert(argumentMap.contains(Symbol(ClientArgumentKeys.PolicyName)))
    assert(argumentMap.contains(Symbol(ClientArgumentKeys.PolicyKey)))
    assert(argumentMap.contains(Symbol(ClientArgumentKeys.MessageLength)))
    assert(argumentMap(Symbol(ClientArgumentKeys.MessageLength)).asInstanceOf[Int] > 0)
  }
}

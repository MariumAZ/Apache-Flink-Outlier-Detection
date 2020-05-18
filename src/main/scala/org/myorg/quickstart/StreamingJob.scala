/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart

import java.util.Properties

import org.apache.flink.shaded.zookeeper.org.apache.zookeeper.jute.compiler.JString
import org.apache.flink.shaded.zookeeper.org.apache.zookeeper.jute.compiler.JInt
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
//import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema
//import org.apache.flink.api.common.serialization.JSONDeserializationSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.functions.RuntimeContext
import org.elasticsearch.client.Requests
import scala.util.parsing.json.JSON
//import org.apache.flink.streaming.util.serialization.{JSONDeserializationSchema,SimpleStringSchema}
/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
object StreamingJob {

  //Defining the threshold we will be using later
  var threshold = 0.3

  def main(args: Array[String]) {

    /* set up the streaming execution environment
        we will be using Event Time */

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "samplegroup")

    //Reading from Kafka source


    case class big(uid: String, timestamp: String, ip: String)
    val display = env.addSource(new FlinkKafkaConsumer[String]("displays", new SimpleStringSchema(), properties))
    val click = env.addSource(new FlinkKafkaConsumer[String]("clicks", new SimpleStringSchema(), properties))


    case class clicks(uid: String,
                    ip: String,
                    Timestamp:String
                   )

    def parseClick(jsonString: String) : clicks= {

      val jsonMap = JSON.parseFull(jsonString).asInstanceOf[Map[String, Any]]
      // On extrait
      val uid= jsonMap.get("uid").get.asInstanceOf[String]
      val ip = jsonMap.get("ip").get.asInstanceOf[String]
      val Timestamp = jsonMap.get("timestamp").get.asInstanceOf[String]
     return clicks(uid,ip,Timestamp)
      }

     val c =click.map(x=>parseClick(x))
       .map(x=>((x.uid),1))
       .print()
       /*
         .keyBy(0)
         .timeWindow(Time.seconds(2))
         .sum(1)
         .print()
*/









    /*
     // Defining a function that will return a tuple
    def keyuid(l: Array[String]): (String, Long, String) = {
      val uid = l(1).split(":")(1)
      val timestamp = l(2).split(":")(1).toLong * 1000 //convert Long to ms
      val ip = l(3).split(":")(1)
      return (uid, timestamp, ip)
    }

    // the first pattern will be users who use same ip but change uids so we will group by ip clicks and displays.
    val cli_ip = click.map(_.split(","))
      .map(x => (keyuid(x), 1.0)).assignAscendingTimestamps {
      case ((_, timestamp, _), _) => timestamp
    }
      .keyBy { elem =>
        elem match {
          case ((_, _, ip), _) => ip
        }
      }
      .timeWindow(Time.minutes(15))
      .sum(1)


    val dis_ip = display.map(_.split(","))
      .map(x => (keyuid(x), 1.0)).assignAscendingTimestamps {
      case ((_, timestamp, _), _) => timestamp
    }
      .keyBy { elem =>
        elem match {
          case ((_, _, ip), _) => ip
        }
      }
      .timeWindow(Time.minutes(15))
      .sum(1)

    // the second pattern will be users who change their ips keeping same uid so we are going to group them by uid.
    val dis_uid = display.map(_.split(","))
      .map(x => (keyuid(x), 1.0)).assignAscendingTimestamps {
      case ((_, timestamp, _), _) => timestamp
    }
      .keyBy { elem =>
        elem match {
          case ((uid, _, _), _) => uid
        }
      }
      .timeWindow(Time.minutes(15))
      .sum(1)

    val cli_uid = click.map(_.split(","))
      .map(x => (keyuid(x), 1.0)).assignAscendingTimestamps {
      case ((_, timestamp, _), _) => timestamp
    }
      .keyBy { elem =>
        elem match {
          case ((uid, _, _), _) => uid
        }
      }
      .timeWindow(Time.minutes(15))
      .sum(1)


    // Join Streams


    //Detect first pattern
    var join_ip = dis_ip.join(cli_ip).where(x => x._1._3).equalTo(x => x._1._3)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .apply((a, b) => (b._1._3, b._2 / a._2))
      .filter(x => x._2 > threshold)
      .print()

    //Detect second pattern
    var join_uid1 = dis_uid.join(cli_uid).where(x => x._1._1).equalTo(x => x._1._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .apply((a, b) => (b._1._1, b._2 / a._2))
      .filter(x => x._2 > threshold)
      .print()


    //Visualisation

    //defining a function that maps all the field of click and display streams

    def field(l: Array[String]): (String,String,Long,String,String) = {
      val event=l(0).split(":")(1)
      val uid = l(1).split(":")(1)
      val timestamp = l(2).split(":")(1).toLong
      val ip = l(3).split(":")(1)
      val impression=l(0).split(":")(1)
      return (event,uid,timestamp,ip,impression)
    }
    val read_click=click.map(_.split(",")).map(x=>field(x))
    val read_display=display.map(_.split(",")).map(x=>field(x))

    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"))
    httpHosts.add(new HttpHost("10.2.3.1", 9200, "http"))
    val esSinkBuilder = new ElasticsearchSink.Builder[(String,String,Long,String,String)](
      httpHosts,
      new ElasticsearchSinkFunction[(String,String,Long,String,String)] {
        def process(x: (String,String,Long,String,String), ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = new java.util.HashMap[String,Any]
          json.put("event", x._1)
          json.put("uid", x._2)
          json.put("timestamp", x._3)
          json.put("ip", x._4)
          json.put("impression", x._5)
          val rqst: IndexRequest = Requests.indexRequest
            .index("clicks")
            .source(json)
          indexer.add(rqst)
        }
      }
    )

    // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
    esSinkBuilder.setBulkFlushMaxActions(2)

    // finally, build and add the sink to the job's pipeline
    read_click.addSink(esSinkBuilder.build)
    read_display.addSink(esSinkBuilder.build)

    



    // execute program
*/
    env.execute("Flink Streaming Scala API Skeleton")
  }

}
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

package org.apache.predictionio.data

import akka.stream.Materializer
import akka.stream.alpakka.elasticsearch.WriteMessage
import akka.stream.alpakka.elasticsearch.scaladsl.{ElasticsearchSink, ElasticsearchSource}
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import spray.json.DefaultJsonProtocol._

import scala.concurrent.Future

class ElasticsearchAccessKeys(implicit m: Materializer) extends AccessKeys {
  private implicit val client = RestClient.builder(new HttpHost("localhost", 9200)).build()
  private implicit val format = jsonFormat2(AccessKey)

  override def init(): Future[Int] = ???

  def close(): Unit = {
    client.close()
  }

  def create(accessKey: AccessKey): Future[Int] = {
    Source(List(accessKey))
      .via(
        Flow[AccessKey].map { key =>
          WriteMessage.createIndexMessage(source = key)
        }
      )
      .runWith(
        ElasticsearchSink.create[AccessKey]("pio-accesskeys", "_doc")
      )
    // TODO
    Future.successful(1)
  }

  def find(key: String): Future[Option[AccessKey]] = {
    ElasticsearchSource.typed[AccessKey](
      "pio-accesskeys",
      "_doc",
      """{"match_all": {}}"""
    ).map { message =>
      message.source
    }
    .runWith(Sink.headOption)
  }
}

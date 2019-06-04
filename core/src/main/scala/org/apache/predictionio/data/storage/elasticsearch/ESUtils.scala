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

package org.apache.predictionio.data.storage.elasticsearch

import scala.collection.JavaConverters._

import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.elasticsearch.client.RestClient
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import org.apache.http.util.EntityUtils
import org.apache.predictionio.data.storage.StorageClientConfig
import org.apache.http.HttpHost

object ESUtils {
  val scrollLife = "1m"

  def getAll[T: Manifest](
                           client: RestClient,
                           index: String,
                           estype: String,
                           query: String)(
                           implicit formats: Formats): Seq[T] = {
    getDocAll(client, index, estype, query).map(x => x.extract[T])
  }

  def getDocAll(
                 client: RestClient,
                 index: String,
                 estype: String,
                 query: String)(
                 implicit formats: Formats): Seq[JValue] = {

    @scala.annotation.tailrec
    def scroll(scrollId: String, hits: Seq[JValue], results: Seq[JValue]): Seq[JValue] = {
      if (hits.isEmpty) results
      else {
        val json = ("scroll" -> scrollLife) ~ ("scroll_id" -> scrollId)
        val scrollBody = new NStringEntity(compact(render(json)), ContentType.APPLICATION_JSON)
        val response = client.performRequest(
          "POST",
          "/_search/scroll",
          Map.empty[String, String].asJava,
          scrollBody)
        val responseJValue = parse(EntityUtils.toString(response.getEntity))
        scroll((responseJValue \ "_scroll_id").extract[String],
          (responseJValue \ "hits" \ "hits").extract[Seq[JValue]],
          results ++ hits.map(h => (h \ "_source").extract[JValue]))
      }
    }

    val entity = new NStringEntity(query, ContentType.APPLICATION_JSON)
    val response = client.performRequest(
      "POST",
      s"/$index/$estype/_search",
      Map("scroll" -> scrollLife).asJava,
      entity)
    val responseJValue = parse(EntityUtils.toString(response.getEntity))
    scroll((responseJValue \ "_scroll_id").extract[String],
      (responseJValue \ "hits" \ "hits").extract[Seq[JValue]],
      Nil)
  }

  def createIndex(
                   client: RestClient,
                   index: String): Unit = {
    client.performRequest(
      "HEAD",
      s"/$index",
      Map.empty[String, String].asJava).getStatusLine.getStatusCode match {
      case 404 =>
        client.performRequest(
          "PUT",
          s"/$index",
          Map.empty[String, String].asJava)
      case 200 =>
      case _ =>
        throw new IllegalStateException(s"/$index is invalid.")
    }
  }

  def createMapping(
                     client: RestClient,
                     index: String,
                     estype: String,
                     json: String): Unit = {
    client.performRequest(
      "HEAD",
      s"/$index/_mapping/$estype",
      Map.empty[String, String].asJava).getStatusLine.getStatusCode match {
      case 404 =>
        val entity = new NStringEntity(json, ContentType.APPLICATION_JSON)
        client.performRequest(
          "PUT",
          s"/$index/_mapping/$estype",
          Map.empty[String, String].asJava,
          entity)
      case 200 =>
      case _ =>
        throw new IllegalStateException(s"/$index/$estype is invalid: $json")
    }
  }

  def getHttpHosts(config: StorageClientConfig): Seq[HttpHost] = {
    val hosts = config.properties.get("HOSTS").
      map(_.split(",").toSeq).getOrElse(Seq("localhost"))
    val ports = config.properties.get("PORTS").
      map(_.split(",").toSeq.map(_.toInt)).getOrElse(Seq(9200))
    val schemes = config.properties.get("SCHEMES").
      map(_.split(",").toSeq).getOrElse(Seq("http"))
    (hosts, ports, schemes).zipped.map((h, p, s) => new HttpHost(h, p, s))
  }

}
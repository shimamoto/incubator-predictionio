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

import akka.{Done, NotUsed}
import akka.stream.scaladsl.Source
import spray.json.JsValue

import scala.concurrent.Future

trait Events {
  val uniqueField = "pio_id"

  def init(): Future[Int]

  def close(): Unit

  def create(event: JsValue, repository: String): Future[Done]

  def create(events: Source[JsValue, NotUsed], repository: String): Future[Done]

  def find(repository: String): Source[JsValue, NotUsed]

}

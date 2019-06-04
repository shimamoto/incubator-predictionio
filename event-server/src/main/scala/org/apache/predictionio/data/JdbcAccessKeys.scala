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
import akka.stream.alpakka.slick.scaladsl.SlickSession
import slick.jdbc.GetResult

import scala.concurrent.Future

class JdbcAccessKeys(implicit m: Materializer) extends AccessKeys {
  private implicit val session = SlickSession.forConfig("slick-postgres")
  private val schema = "pio"
  private val table = "accesskeys"

  import session.profile.api._

  def init(): Future[Int] = ???

  def close(): Unit = {
    session.close()
  }

  def create(accessKey: AccessKey): Future[Int] = {
    session.db.run(
      sqlu"INSERT INTO #$schema.#$table (accesskey, repository) VALUES(${accessKey.key}, ${accessKey.repository})")
  }

  def find(key: String): Future[Option[AccessKey]] = {
    implicit val rconv = GetResult(r => AccessKey(r.<<[String], r.<<[String]))
    session.db.run(
      sql"SELECT accesskey, repository FROM #$schema.#$table WHERE accesskey = $key"
        .as[AccessKey].headOption)
  }
}

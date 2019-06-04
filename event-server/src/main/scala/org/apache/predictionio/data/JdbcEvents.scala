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
import akka.stream.Materializer
import akka.stream.alpakka.slick.scaladsl._
import akka.stream.scaladsl.Source
import slick.jdbc.{GetResult, SQLActionBuilder, SimpleJdbcAction}
import spray.json.{JsNull, JsNumber, JsObject, JsString, JsValue}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class JdbcEvents(implicit m: Materializer) extends Events {
  private implicit val session = SlickSession.forConfig("slick-postgres")
  private val schema = "pio"
  private val tables = {
    val dbio = SimpleJdbcAction { ctx =>
      val meta = ctx.session.metaData
      val rs = meta.getColumns(null, schema, "%", "%")

      def tables(m: Map[String, Map[String, Int]] = Map.empty): Map[String, Map[String, Int]] =
        if (!rs.next()) m else {
          rs.getString("COLUMN_NAME") match {
            case `uniqueField` => tables(m)
            case column =>
              val table = rs.getString("TABLE_NAME")
              val columns = m.getOrElse(table, Map.empty)
              tables(m + (table -> (columns + (column -> rs.getInt("DATA_TYPE")))))
          }
        }

      tables()
    }
    Await.result(session.db.run(dbio), Duration.fromNanos(10 * 1000000000))
  }

  import session.profile.api._

  override def init(): Future[Int] = ???

  def close(): Unit = {
    session.close()
  }

  def create(event: JsValue, repository: String): Future[Done] = {
    create(Source(List(event)), repository)
  }

  def create(events: Source[JsValue, NotUsed], repository: String): Future[Done] = {
    val table = repository
    val columns = tables(table).keySet.toList

    events
      .runWith(
        Slick.sink { json =>
          val fields = json.asJsObject.fields

          SimpleJdbcAction { ctx =>
            ctx.session.withPreparedStatement(s"INSERT INTO $schema.$table ${
              columns.mkString("(", ",", ")")} VALUES ${
              Nil.padTo(columns.size, "?").mkString("(", ",", ")")}") { st =>

              columns.zipWithIndex.foreach { case (column, i) =>
                tables(table)(column) match {
                  case java.sql.Types.VARCHAR =>
                    st.setString(i + 1, fields.get(column).map(_.asInstanceOf[JsString].value).getOrElse(null))
                  case java.sql.Types.INTEGER =>
                    st.setBigDecimal(i + 1, fields.get(column).map(_.asInstanceOf[JsNumber].value.bigDecimal).getOrElse(null))
                  case _ =>
                    st.setObject(i + 1, fields.get(column).map(_.asInstanceOf[JsString].value).getOrElse(null))
                }
              }
              st.executeUpdate()
            }
          }
        }
      )
  }

  def find(repository: String): Source[JsValue, NotUsed] = {
    val table = repository
    val columns = tables(table).keySet.toList

    implicit val rconv = GetResult { r =>
      val fields = columns.map { column =>
        tables(table)(column) match {
          case java.sql.Types.VARCHAR =>
            val value = r.<<?[String].map(JsString.apply).getOrElse(JsNull)
            column -> value
          case java.sql.Types.INTEGER =>
            val value = r.<<?[Int].map(JsNumber.apply).getOrElse(JsNull)
            column -> value
          case _ =>
            val value = r.nextObjectOption().map(x => JsString(x.toString)).getOrElse(JsNull)
            column -> value
        }
      }.toMap

      JsObject(fields)
    }

    Slick
      .source(
        SQLActionBuilder(List(s"SELECT ${columns.mkString(",")} FROM $schema.$table"), null).as[JsObject]
      )
  }

}

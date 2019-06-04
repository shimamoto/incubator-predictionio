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

package org.apache.predictionio.api

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.management.AkkaManagement
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.Http
import akka.http.scaladsl.common.EntityStreamingSupport
import akka.http.scaladsl.model.headers.{HttpChallenge, OAuth2BearerToken}
import akka.http.scaladsl.server.directives.{AuthenticationDirective, AuthenticationResult}
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import spray.json.JsValue
import org.apache.predictionio.data._

import scala.concurrent.Future

class EventServer(message: String, events: Events, accessKeys: AccessKeys) {

  private implicit val jsonStreamingSupport = EntityStreamingSupport.json()

  val route =
    pathSingleSlash {
      get {
        complete(message)
      }
    } ~
    path("api" / "v1" / "event") {
      put {
        authenticate { key =>
          entity(as[JsValue]) { json =>
            onSuccess(events.create(json, key.repository)) { done =>
              complete(StatusCodes.Created)
            }
          }
        }
      }
    } ~
    path("api" / "v1" / "events") {
      authenticate { key =>
        entity(asSourceOf[JsValue]) { json =>
          onSuccess(events.create(json, key.repository)) { done =>
            complete(StatusCodes.Created)
          }
        }
      }
    } ~
    path("api" / "v1" / "events") {
      authenticate { key =>
        complete(events.find(key.repository))
      }
    }

  private def authenticate: AuthenticationDirective[AccessKey] = {
    extractExecutionContext.flatMap { implicit ec =>
      authenticateOrRejectWithChallenge[OAuth2BearerToken, AccessKey] {
        case Some(OAuth2BearerToken(token)) =>
          accessKeys.find(token).map {
            case Some(key) => AuthenticationResult.success(key)
            case _ => AuthenticationResult.failWithChallenge(
              HttpChallenge("bearer", None, Map("error" -> "invalid_token")))
          }
        case _ =>
          Future.successful(AuthenticationResult.failWithChallenge(
            HttpChallenge("bearer", None, Map("error" -> "invalid_token")))
          )
      }
    }
  }

}

object EventServer extends App {
  lazy val config = ConfigFactory.load()
  implicit val system = ActorSystem("akka-simple-cluster")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher
  implicit val cluster = Cluster(system)

  AkkaManagement(system).start()
  ClusterBootstrap(system).start()

  val (events, accessKeys) =
    if (config.hasPath("elasticsearch"))
      (new ElasticsearchEvents(), new ElasticsearchAccessKeys())
    else
      (new JdbcEvents(), new JdbcAccessKeys())
  system.registerOnTermination(() => {
    events.close()
    accessKeys.close()
  })
//  storage.start("book")

  Http().bindAndHandle(
    new EventServer(config.getString("application.api.hello-message"), events, accessKeys).route,
    config.getString("application.api.host"),
    config.getInt("application.api.port")
  )

}

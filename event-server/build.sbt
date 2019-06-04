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

name := "apache-predictionio-event-server"
version := "0.0.1-SNAPSHOT"
scalaVersion := "2.12.8"

resolvers += Resolver.bintrayRepo("tanukkii007", "maven")

val akkaVersion = "2.5.19"
val akkaHttpVersion = "10.1.7"
val akkaManagementVersion = "0.20.0"
val alpakkaVersion = "1.0-M2"

libraryDependencies ++=Seq(
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.lightbend.akka.management" %% "akka-management-cluster-http" % akkaManagementVersion,
  "com.lightbend.akka.management" %% "akka-management-cluster-bootstrap" % akkaManagementVersion,
  "com.lightbend.akka.discovery" %% "akka-discovery-kubernetes-api" % akkaManagementVersion,
  "com.lightbend.akka.discovery" %% "akka-discovery-dns" % akkaManagementVersion,
  "com.github.TanUkkii007" %% "akka-cluster-custom-downing" % "0.0.12",

  "com.lightbend.akka" %% "akka-stream-alpakka-elasticsearch" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-slick" % alpakkaVersion,
  "org.postgresql" % "postgresql" % "42.2.5",
  "com.lightbend.akka" %% "akka-stream-alpakka-hbase" % alpakkaVersion,
  "org.slf4j" % "slf4j-log4j12" % "1.7.26"
)

enablePlugins(JavaServerAppPackaging, DockerPlugin)
dockerBaseImage := "openjdk:8"
dockerUsername := Some("shimamoto")

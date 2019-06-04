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

import java.security.SecureRandom

import org.apache.commons.codec.binary.Base64

import scala.concurrent.Future

case class AccessKey(key: String, repository: String)

trait AccessKeys {

  def init(): Future[Int]

  def close(): Unit

  def create(accessKey: AccessKey): Future[Int]

  def find(key: String): Future[Option[AccessKey]]

  /** Default implementation of key generation */
  def generateKey: String = {
    val sr = new SecureRandom
    val srBytes = Array.fill(48)(0.toByte)
    sr.nextBytes(srBytes)
    Base64.encodeBase64URLSafeString(srBytes) match {
      case x if x startsWith "-" => generateKey
      case x => x
    }
  }
}

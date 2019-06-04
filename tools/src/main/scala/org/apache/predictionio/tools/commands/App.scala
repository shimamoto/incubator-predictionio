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

package org.apache.predictionio.tools.commands

import org.apache.predictionio.data.storage
import org.apache.predictionio.data.storage.Channel
import org.apache.predictionio.tools.EitherLogging
import org.apache.predictionio.tools.ReturnTypes._


object App extends EitherLogging {

  def create(
    name: String,
    id: Option[Int] = None,
    description: Option[String] = None): Expected[storage.App] = {

    val apps = storage.Storage.getMetaDataApps()

    apps.getByName(name) map { app =>
      val errStr = s"App ${name} already exists. Aborting."
      error(errStr)
      errStr
    } orElse {
      id.flatMap { id =>
        apps.get(id) map { app =>
          val errStr = s"App ID ${id} already exists and maps to the app '${app.name}'. " +
            "Aborting."
          error(errStr)
          errStr
        }
      }
    } map {err => Left(err)} getOrElse {
      val newApp = storage.App(
        id = id.getOrElse(0),
        name = name,
        description = description)
      val appid = apps.insert(newApp)

      appid map { id =>
        Right(newApp.copy(id = id))
      } getOrElse {
        logAndFail(s"Unable to create new app.")
      }
    }
  }

  def list: Seq[storage.App] = {
    storage.Storage.getMetaDataApps.getAll().sortBy(_.name)
  }

  def show(appName: String): Expected[(storage.App, Seq[Channel])] = {
    val apps = storage.Storage.getMetaDataApps
    val channels = storage.Storage.getMetaDataChannels

    apps.getByName(appName) map { app =>
      Right(
        (app, channels.getByAppid(app.id))
      )
    } getOrElse {
      logAndFail(s"App ${appName} does not exist. Aborting.")
    }
  }

  def delete(name: String): MaybeError = {
    show(name).right.flatMap { case (app: storage.App, channels: Seq[Channel]) =>

      val delChannelStatus: MaybeError =
        channels.map { ch =>
          try {
            storage.Storage.getMetaDataChannels.delete(ch.id)
            info(s"Deleted channel ${ch.name}")
            None
          } catch {
            case e: Exception =>
              val errStr = s"Error deleting channel ${ch.name}."
              error(errStr, e)
              Some(errStr)
          }
        }
        .flatten
        .reduceOption(_ + "\n" + _)
        .map(Left(_)) getOrElse Success

        if (delChannelStatus.isLeft) {
          return delChannelStatus
        }

        try {
          storage.Storage.getMetaDataApps.delete(app.id)
          info(s"Deleted app ${app.name}.")
        } catch {
          case e: Exception =>
            logAndFail(s"Error deleting app ${app.name}. Aborting.")
        }
        logAndSucceed("Done.")
    }
  }

  def channelNew(appName: String, newChannel: String): Expected[Channel] = {
    val chanStorage = storage.Storage.getMetaDataChannels

    show(appName).right flatMap { case (app: storage.App, channels: Seq[Channel]) =>
      if (channels.find(ch => ch.name == newChannel).isDefined) {
        logAndFail(s"""Channel ${newChannel} already exists.
                    |Unable to create new channel.""".stripMargin)
      } else if (!storage.Channel.isValidName(newChannel)) {
        logAndFail(s"""Unable to create new channel.
                    |The channel name ${newChannel} is invalid.
                    |${storage.Channel.nameConstraint}""".stripMargin)
      } else {

        val channel = Channel(
          id = 0,
          appid = app.id,
          name = newChannel)

        chanStorage.insert(channel) map { chanId =>
          Right(channel.copy(id = chanId))

        } getOrElse {
          logAndFail(s"Unable to create new channel.")
        }
      }
    }
  }

  def channelDelete(appName: String, deleteChannel: String): MaybeError = {
    val chanStorage = storage.Storage.getMetaDataChannels

    show(appName).right.flatMap { case (app: storage.App, channels: Seq[Channel]) =>
      val foundChannel = channels.find(ch => ch.name == deleteChannel)
      foundChannel match {
        case None =>
          logAndFail(s"""Unable to delete channel
                        |Channel ${deleteChannel} doesn't exists.""".stripMargin)
        case Some(channel) =>
          try {
            chanStorage.delete(channel.id)
            logAndSucceed(s"Deleted channel: ${deleteChannel}.")
          } catch {
            case e: Exception =>
              logAndFail((s"""Unable to delete channel.
               |The channel ${deleteChannel} CANNOT be used!
               |Please run 'pio app channel-delete ${app.name} ${deleteChannel}'""" +
                " to delete this channel again!").stripMargin)
          }
      }
    }
  }
}

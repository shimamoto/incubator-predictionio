/** Copyright 2014 TappingStone, Inc.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package io.prediction.data.storage.hbase

import io.prediction.data.storage.Event
import io.prediction.data.storage.EventValidation
import io.prediction.data.storage.Events
import io.prediction.data.storage.DataMap

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.RegexStringComparator
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.BinaryComparator

import org.json4s.DefaultFormats
import org.json4s.JObject
import org.json4s.native.Serialization.{ read, write }

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import org.apache.commons.codec.binary.Base64
import java.security.MessageDigest

import java.util.UUID

/* common utility function for acessing EventsStore in HBase */
object HBEventsUtil {

  implicit val formats = DefaultFormats

  def tableName(namespace: String, appId: Int) = s"${namespace}:events_${appId}"
  //val table = "events"

  // column nams for "e" column family
  val colNames: Map[String, Array[Byte]] = Map(
    "event" -> "e",
    "entityType" -> "ety",
    "entityId" -> "eid",
    "targetEntityType" -> "tety",
    "targetEntityId" -> "teid",
    "properties" -> "p",
    "predictionKey" -> "pk",
    "eventTime" -> "et",
    "eventTimeZone" -> "etz",
    "creationTime" -> "ct",
    "creationTimeZone" -> "ctz"
  ).mapValues(Bytes.toBytes(_))

  val md5 = MessageDigest.getInstance("MD5")

  def hash(entityType: String, entityId: String): Array[Byte] = {
    val s = entityType + "-" + entityId
    md5.digest(Bytes.toBytes(s))
  }

  class RowKey(
    val b: Array[Byte]
  ) {
    require((b.size == 32), s"Incorrect b size: ${b.size}")
    lazy val entityHash: Array[Byte] = b.slice(0, 16)
    lazy val millis: Long = Bytes.toLong(b.slice(16, 24))
    lazy val uuidLow: Long = Bytes.toLong(b.slice(24, 32))

    lazy val toBytes: Array[Byte] = b

    override def toString: String = {
      Base64.encodeBase64URLSafeString(toBytes)
    }
  }

  object RowKey {
    def apply(
      entityType: String,
      entityId: String,
      millis: Long,
      uuidLow: Long): RowKey = {
        // add UUID least significant bits for multiple actions at the same time
        // (UUID's most significantbits are actually timestamp,
        // use eventTime instead).
        val b = hash(entityType, entityId) ++
          Bytes.toBytes(millis) ++ Bytes.toBytes(uuidLow)
        new RowKey(b)
      }

    // get RowKey from string representation
    def apply(s: String): RowKey = {
      try {
        apply(Base64.decodeBase64(s))
      } catch {
        case e: Exception => throw new RowKeyException(
          s"Failed to convert String ${s} to RowKey because ${e}", e)
      }
    }

    def apply(b: Array[Byte]): RowKey = {
      if (b.size != 32) {
        val bString = b.mkString(",")
        throw new RowKeyException(
          s"Incorrect byte array size. Bytes: ${bString}.")
      }
      new RowKey(b)
    }

  }

  class RowKeyException(val msg: String, val cause: Exception)
    extends Exception(msg, cause) {
      def this(msg: String) = this(msg, null)
    }

  /*
  case class PartialRowKey(val appId: Int, val millis: Option[Long] = None) {
    val toBytes: Array[Byte] = {
      Bytes.toBytes(appId) ++
        (millis.map(Bytes.toBytes(_)).getOrElse(Array[Byte]()))
    }
  }*/

  case class PartialRowKey(entityType: String, entityId: String,
    millis: Option[Long] = None) {
    val toBytes: Array[Byte] = {
      hash(entityType, entityId) ++
        (millis.map(Bytes.toBytes(_)).getOrElse(Array[Byte]()))
    }
  }

  def eventToPut(event: Event, appId: Int): (Put, RowKey) = {
    // TOOD: use real UUID. not psuedo random
    val uuidLow: Long = UUID.randomUUID().getLeastSignificantBits
    val rowKey = RowKey(
      entityType = event.entityType,
      entityId = event.entityId,
      millis = event.eventTime.getMillis,
      uuidLow = uuidLow
    )

    val eBytes = Bytes.toBytes("e")
    // use eventTime as HBase's cell timestamp
    val put = new Put(rowKey.toBytes, event.eventTime.getMillis)

    def addStringToE(col: Array[Byte], v: String) = {
      put.add(eBytes, col, Bytes.toBytes(v))
    }

    def addLongToE(col: Array[Byte], v: Long) = {
      put.add(eBytes, col, Bytes.toBytes(v))
    }

    addStringToE(colNames("event"), event.event)
    addStringToE(colNames("entityType"), event.entityType)
    addStringToE(colNames("entityId"), event.entityId)

    event.targetEntityType.foreach { targetEntityType =>
      addStringToE(colNames("targetEntityType"), targetEntityType)
    }

    event.targetEntityId.foreach { targetEntityId =>
      addStringToE(colNames("targetEntityId"), targetEntityId)
    }

    // TODO: make properties Option[]
    if (!event.properties.isEmpty) {
      addStringToE(colNames("properties"), write(event.properties.toJObject))
    }

    event.predictionKey.foreach { predictionKey =>
      addStringToE(colNames("predictionKey"), predictionKey)
    }

    addLongToE(colNames("eventTime"), event.eventTime.getMillis)
    val eventTimeZone = event.eventTime.getZone
    if (!eventTimeZone.equals(EventValidation.defaultTimeZone)) {
      addStringToE(colNames("eventTimeZone"), eventTimeZone.getID)
    }

    addLongToE(colNames("creationTime"), event.creationTime.getMillis)
    val creationTimeZone = event.creationTime.getZone
    if (!creationTimeZone.equals(EventValidation.defaultTimeZone)) {
      addStringToE(colNames("creationTimeZone"), creationTimeZone.getID)
    }

    // can use zero-length byte array for tag cell value
    (put, rowKey)
  }

  def resultToEvent(result: Result, appId: Int): Event = {
    val rowKey = RowKey(result.getRow())

    val eBytes = Bytes.toBytes("e")
    //val e = result.getFamilyMap(eBytes)

    def getStringCol(col: String): String = {
      val r = result.getValue(eBytes, colNames(col))
      require(r != null,
        s"Failed to get value for column ${col}. " +
        s"Rowkey: ${rowKey.toString} " +
        s"StringBinary: ${Bytes.toStringBinary(result.getRow())}.")

      Bytes.toString(r)
    }

    def getLongCol(col: String): Long = {
      val r = result.getValue(eBytes, colNames(col))
      require(r != null,
        s"Failed to get value for column ${col}. " +
        s"Rowkey: ${rowKey.toString} " +
        s"StringBinary: ${Bytes.toStringBinary(result.getRow())}.")

      Bytes.toLong(r)
    }

    def getOptStringCol(col: String): Option[String] = {
      val r = result.getValue(eBytes, colNames(col))
      if (r == null)
        None
      else
        Some(Bytes.toString(r))
    }

    def getTimestamp(col: String): Long = {
      result.getColumnLatestCell(eBytes, colNames(col)).getTimestamp()
    }

    val event = getStringCol("event")
    val entityType = getStringCol("entityType")
    val entityId = getStringCol("entityId")
    val targetEntityType = getOptStringCol("targetEntityType")
    val targetEntityId = getOptStringCol("targetEntityId")
    val properties: DataMap = getOptStringCol("properties")
      .map(s => DataMap(read[JObject](s))).getOrElse(DataMap())
    val predictionKey = getOptStringCol("predictionKey")
    val eventTimeZone = getOptStringCol("eventTimeZone")
      .map(DateTimeZone.forID(_))
      .getOrElse(EventValidation.defaultTimeZone)
    val eventTime = new DateTime(
      getLongCol("eventTime"), eventTimeZone)
    val creationTimeZone = getOptStringCol("creationTimeZone")
      .map(DateTimeZone.forID(_))
      .getOrElse(EventValidation.defaultTimeZone)
    val creationTime: DateTime = new DateTime(
      getLongCol("creationTime"), creationTimeZone)

    Event(
      eventId = Some(RowKey(result.getRow()).toString),
      event = event,
      entityType = entityType,
      entityId = entityId,
      targetEntityType = targetEntityType,
      targetEntityId = targetEntityId,
      properties = properties,
      eventTime = eventTime,
      tags = Seq(),
      appId = appId,
      predictionKey = predictionKey,
      creationTime = creationTime
    )
  }


  def createScan(
    startTime: Option[DateTime],
    untilTime: Option[DateTime],
    entityType: Option[String],
    entityId: Option[String],
    reversed: Option[Boolean] = Some(false)): Scan = {

    val scan: Scan = new Scan()

    (entityType, entityId) match {
      case (Some(et), Some(eid)) => {
        val start = PartialRowKey(et, eid,
          startTime.map(_.getMillis)).toBytes
        // if no untilTime, stop when reach next bytes of entityTypeAndId
        val stop = PartialRowKey(et, eid,
          untilTime.map(_.getMillis).orElse(Some(-1))).toBytes

        if (reversed.getOrElse(false)) {
          // Reversed order.
          // If you specify a startRow and stopRow,
          // to scan in reverse, the startRow needs to be lexicographically
          // after the stopRow.
          scan.setStartRow(stop)
          scan.setStopRow(start)
          scan.setReversed(true)
        } else {
          scan.setStartRow(start)
          scan.setStopRow(stop)
        }
      }
      case (_, _) => {
        val minTime: Long = startTime.map(_.getMillis).getOrElse(0)
        val maxTime: Long = untilTime.map(_.getMillis).getOrElse(Long.MaxValue)
        scan.setTimeRange(minTime, maxTime)
        if (reversed.getOrElse(false)) {
          scan.setReversed(true)
        }
      }
    }

    if ((entityType != None) || (entityId != None)) {
      val filters = new FilterList()
      val eBytes = Bytes.toBytes("e")
      entityType.foreach { et =>
        //val compType = new RegexStringComparator("^"+etype+"$")
        val compType = new BinaryComparator(Bytes.toBytes(et))
        val filterType = new SingleColumnValueFilter(
          eBytes, colNames("entityType"), CompareOp.EQUAL, compType)
        filters.addFilter(filterType)
      }
      entityId.foreach { eid =>
        //val compId = new RegexStringComparator("^"+eid+"$")
        val compId = new BinaryComparator(Bytes.toBytes(eid))
        val filterId = new SingleColumnValueFilter(
          eBytes, colNames("entityId"), CompareOp.EQUAL, compId)
        filters.addFilter(filterId)
      }
      scan.setFilter(filters)
    }

    scan
  }

}

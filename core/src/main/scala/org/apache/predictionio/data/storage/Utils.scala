package org.apache.predictionio.data.storage

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

/** Backend-agnostic storage utilities. */
private[predictionio] object Utils {

  def stringToDateTime(dt: String): DateTime =
    ISODateTimeFormat.dateTimeParser.parseDateTime(dt)
}

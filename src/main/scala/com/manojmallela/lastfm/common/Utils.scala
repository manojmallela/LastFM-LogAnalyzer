package com.manojmallela.lastfm.common

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.SparkSession

object Utils {


  def stringToTimestamp(dateString: String): Timestamp = {
    Timestamp.valueOf(LocalDateTime.parse(dateString, DateTimeFormatter.ISO_DATE_TIME))
  }

  def longToTimestamp(tsValue: Long): Timestamp = {
    new Timestamp(tsValue)
  }


  def createSparkSession(appName: String, master: String): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .master(master)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
  }

}

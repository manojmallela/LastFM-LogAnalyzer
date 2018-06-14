package com.manojmallela.lastfm.common

import java.sql.Timestamp

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.collection.mutable

object DAO {


  case class InputArgs(sparkMaster: String = "local[*]", executionMode: Int = 1, inputFilePath: String = "", outputFilePath: String = "")


  case class UserActivity(userId: String, timestamp: Timestamp, artistId: String,
                          artistName: String, trackId: String, trackName: String)


  case class UserSession(songsPlayed: mutable.Seq[String], startedAt: Long, endedAt: Long) {

    def updateSession(track: String, updatedTimestamp: Long): UserSession = {
      UserSession(songsPlayed :+ track, startedAt, updatedTimestamp)
    }

    def sessionLength: Long = endedAt - startedAt

    override def toString: String = {
      s"startedAt: $startedAt, endedAt: $endedAt, songsPlayed: ${songsPlayed.mkString(",")}"
    }

  }

  def getInputFileSchema: StructType = StructType(Seq(
    StructField("userId", DataTypes.StringType, nullable = false),
    StructField("timestamp", DataTypes.TimestampType),
    StructField("musicbrainz-artist-id", DataTypes.StringType),
    StructField("artist-name", DataTypes.StringType),
    StructField("musicbrainz-track-id", DataTypes.StringType),
    StructField("track-name", DataTypes.StringType)
  ))

}

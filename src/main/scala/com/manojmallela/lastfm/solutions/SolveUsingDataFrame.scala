package com.manojmallela.lastfm.solutions

import com.manojmallela.lastfm.common.DAO
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes


/**
  * An Apache Spark application to process data from Last.FM
  *
  */


class SolveUsingDataFrame(spark: SparkSession, inputFilePath: String) {

  import spark.implicits._

  private var userActivityDF: DataFrame = _
  private val INPUT_FORMAT_DELIMITER = "\t"

  def executeTasksABC(outputPath: String): Unit = {

    distinctSongsCount().write.parquet(s"$outputPath/distinctSongsCount/")
    top100Songs().write.parquet(s"$outputPath/top100Songs/")
    top10Sessions().write.parquet(s"$outputPath/top10Sessions/")
  }


  def init(): Unit = {

    val userActivityRaw = spark
      .read
      .option("delimiter", INPUT_FORMAT_DELIMITER)
      .schema(DAO.getInputFileSchema)
      .csv(inputFilePath)

    // As data is in GMT timezone set SparkSQL timezone for accurate conversion when using `datetime_funcs`
    spark.conf.set("spark.sql.session.timeZone", "GMT")

    val userActivity =
      userActivityRaw
        .withColumn("epoch_timestamp", unix_timestamp($"timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'").cast(DataTypes.LongType))
        .withColumnRenamed("musicbrainz-artist-id", "artistId")
        .withColumnRenamed("artist-name", "artist_name")
        .withColumnRenamed("track-name", "track_name")
        .withColumnRenamed("musicbrainz-track-id", "track_id")
        .orderBy("epoch_timestamp")

    userActivityDF = userActivity
  }


  // A. Create a list of user IDs, along with the number of distinct songs each user has played.
  def distinctSongsCount(): DataFrame = {
    val distinctTrackCount = userActivityDF
      .select($"userId", $"track_name")
      .groupBy("userId")
      .agg(countDistinct("track_name").as("distinct_tracks_count"))
      .sort($"userId")

    distinctTrackCount
  }


  //B. Create a list of the 100 most popular songs (artist and title) in the dataset, with the number of times each was played.
  def top100Songs(): DataFrame = {
    val top100Titles = userActivityDF
      .groupBy("track_name", "artist_name")
      .agg(count("track_name").as("play_count"))
      .sort($"play_count".desc)
      .limit(100)
      .select($"artist_name", $"track_name", $"play_count")

    top100Titles
  }


  // C. Top 10 user sessions
  def top10Sessions(): DataFrame = {
    val userWindowSpec = Window.partitionBy("userId").orderBy($"epoch_timestamp")
    val sessionsDF = userActivityDF
      .withColumn("lag",
        (($"epoch_timestamp" - lag($"epoch_timestamp", 1, 0).over(userWindowSpec))
          >= (20 * 60)).cast(DataTypes.IntegerType))
      .withColumn("user_session_id", concat($"userId", sum("lag").over(userWindowSpec)))
      .sort($"epoch_timestamp".asc)
      .cache()

    val top10Sessions = sessionsDF
      .groupBy("user_session_id", "userId")
      .agg(
        min("epoch_timestamp")
          .alias("session_started_at"),
        max("epoch_timestamp")
          .alias("session_ended_at"),
        collect_list($"track_name").as("tracks_played")
      )
      .withColumn("session_duration",
        col("session_ended_at") - col("session_started_at"))
      .sort($"session_duration".desc)
      .limit(10)
      .select(
        $"userId",
        $"session_started_at".cast("timestamp").cast("string").as("first_song_timestamp"),
        $"session_ended_at".cast("timestamp").cast("string").as("last_song_timestamp"),
        $"tracks_played")

    top10Sessions
  }

}

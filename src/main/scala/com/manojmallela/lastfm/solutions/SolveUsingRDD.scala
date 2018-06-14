package com.manojmallela.lastfm.solutions

import java.sql.Timestamp

import com.manojmallela.lastfm.common.Utils
import com.manojmallela.lastfm.common.DAO.{UserActivity, UserSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

class SolveUsingRDD(spark: SparkSession, filePath: String) {


  private var userActivityRDD: RDD[UserActivity] = _

  def executeTasksABC(outputPath: String): Unit = {
    distinctSongsCount().saveAsTextFile(s"$outputPath/distinctSongsPlayed/")
    top100Songs().saveAsTextFile(s"$outputPath/top100Songs/")
    top10Sessions().saveAsTextFile(s"$outputPath/top10Sessions/")
  }


  def init(): Unit = {

    userActivityRDD = spark.sparkContext
      .textFile(filePath)
      .map(line => line.split("\t"))
      .map { stringArray =>
        UserActivity(
          stringArray(0),
          Utils.stringToTimestamp(stringArray(1)),
          stringArray(2),
          stringArray(3),
          stringArray(4),
          stringArray(5))
      }
  }

  // A. Create a list of user IDs, along with the number of distinct songs each user has played.
  def distinctSongsCount(): RDD[(String, Int)] = {

    val distinctSongsPlayedRDD: RDD[(String, Int)] = userActivityRDD
      .map(userActivity => (userActivity.userId, userActivity.trackName))
      // For this use case groupByKey() has better performance.
      // In general, it is recommended to replace groupByKey with aggregateByKey or reduceByKey.
      .groupByKey()
      .mapValues(_.toList.distinct.size)

    distinctSongsPlayedRDD
  }

  // B. Create a list of the 100 most popular songs (artist and title) in the dataset, with the number of times each was played.
  def top100Songs(): RDD[(String, String, Int)] = {

    val cutOffLimit = 100
    val top100Songs: Array[(String, String, Int)] = userActivityRDD
      .map(userActivity => ((userActivity.trackName, userActivity.artistName), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .take(cutOffLimit)
      .map {
        case ((trackName, artistName), count) => (artistName, trackName, count)
      }

    spark.sparkContext.parallelize(top100Songs)
  }


  def top10Sessions(): RDD[(String, Timestamp, Timestamp, List[String])] = {

    // C. User Session
    val sessionsRDD: RDD[(String, List[UserSession])] = userActivityRDD
      .map(userActivity => (userActivity.userId, (userActivity.timestamp.getTime, userActivity.trackName)))
      .groupByKey()
      .mapValues { iterableTimestampTrackTuple: Iterable[(Long, String)] =>

        val maxTimeGapInSession = 20 * 60 * 1000
        val zeroValue = List.empty[UserSession]
        val sortedActivity = iterableTimestampTrackTuple.toList.sortBy(_._1)
        sortedActivity.foldLeft(zeroValue) { case (list: List[UserSession], (timestamp, track)) =>

          if (list.nonEmpty) {
            // active session exists, update session
            if (timestamp - list.head.endedAt <= maxTimeGapInSession) {
              list.head.updateSession(track, timestamp) :: list.tail
            }
            else {
              // session expired, create new session
              UserSession(mutable.Seq(track), timestamp, timestamp) :: list
            }
          } else {
            // first session
            List(UserSession(mutable.Seq[String](track), timestamp, timestamp))
          }
        }
      }

    val flattenedSessions: RDD[(String, Long, Long, mutable.Seq[String], Long)] =
      sessionsRDD.flatMap { x: (String, List[UserSession]) =>
        x._2.map { y: UserSession =>
          (x._1, y.startedAt, y.endedAt, y.songsPlayed, y.sessionLength)
        }
      }

    val top10Sessions: Array[(String, Timestamp, Timestamp, List[String])] = flattenedSessions
      .sortBy(_._5, ascending = false)
      .take(10)
      .map { z =>
        (z._1, new Timestamp(z._2), new Timestamp(z._3), z._4.toList)
      }

    spark.sparkContext.parallelize(top10Sessions)
  }

}

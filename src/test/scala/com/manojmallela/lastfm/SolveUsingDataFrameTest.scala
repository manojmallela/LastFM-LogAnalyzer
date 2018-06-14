package com.manojmallela.lastfm

import java.sql.Timestamp
import java.util

import com.manojmallela.lastfm.solutions.SolveUsingDataFrame
import com.manojmallela.wpay.lastfm.ExpectedResults.TopSession
import com.manojmallela.wpay.lastfm.common.DAO
import com.manojmallela.wpay.lastfm.solutions.{SolveUsingDataFrame, SolveUsingRDD}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpec, FunSuite}

import scala.collection.mutable

class SolveUsingDataFrameTest extends FunSuite with BeforeAndAfterAll {

  @transient private var _spark: SparkSession = _
  private var _solveUsingDataFrame: SolveUsingDataFrame = _

  override def beforeAll(): Unit = {
    _spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    _solveUsingDataFrame = new SolveUsingDataFrame(_spark, getClass.getResource("/test-data.tsv").toString)
    _solveUsingDataFrame.init()

    super.beforeAll()
  }

  test("Test Task A: count distinct songs played by each userId") {

    val distinctSongsCount = _solveUsingDataFrame.distinctSongsCount()
      .collect()
      .map(row => (row.getString(0), row.getLong(1))).sorted
    assert(distinctSongsCount.sameElements(ExpectedResults.distinctSongCountExpected.sorted))
  }


  test("Test Task B: 100 most popular songs (artist and title)") {

    val top100Songs = _solveUsingDataFrame.top100Songs()
      .collect()
      .map(row => (row.getString(0), row.getString(1), row.getLong(2)))
    // Calculate uniqueness using (artist + title) hashcode
    assert{
      top100Songs.sortBy(x => (x._1 + x._2).hashCode)
        .sameElements(ExpectedResults.top100SongsExpected.sortBy(x => (x._1 + x._2).hashCode))
    }
  }


  test("Test Task C: Top 10 longest sessions") {

    val top10Sessions = _solveUsingDataFrame.top10Sessions()
      .collect()
      .map(row => TopSession(row.getString(0), Timestamp.valueOf(row.getString(1)).toString, Timestamp.valueOf(row.getString(2)).toString, row.getAs[mutable.WrappedArray[String]](3).toList))

    assert(top10Sessions.sortBy(_.userId).sameElements(ExpectedResults.top10SessionsExpected.sortBy(_.userId)))
  }


}

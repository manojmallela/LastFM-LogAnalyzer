package com.manojmallela.lastfm

import com.manojmallela.lastfm.solutions.SolveUsingRDD
import com.manojmallela.lastfm.ExpectedResults.TopSession
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * @author Manoj Mallela
  *
  */
class SolveUsingRDDTest extends FunSuite with BeforeAndAfterAll {

  @transient private var _spark: SparkSession = _
  private var _solveUsingRDDTest: SolveUsingRDD = _

  override def beforeAll(): Unit = {
    _spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    _solveUsingRDDTest = new SolveUsingRDD(_spark, getClass.getResource("/test-data.tsv").toString)
    _solveUsingRDDTest.init()

    super.beforeAll()
  }


  test("Test Task A: count distinct songs played by each userId") {
    val distinctSongsCount = _solveUsingRDDTest.distinctSongsCount().collect()
    assert(distinctSongsCount.sameElements(ExpectedResults.distinctSongCountExpected))
  }


  test("Test Task B: 100 most popular songs (artist and title)") {

    val top100Songs = _solveUsingRDDTest.top100Songs().collect()

    assert {
      top100Songs.sortBy(x => (x._1 + x._2).hashCode)
        .sameElements(ExpectedResults.top100SongsExpected.sortBy(x => (x._1 + x._2).hashCode))
    }
  }


  test("Test Task C: Top 10 longest sessions") {

    val top10Sessions = _solveUsingRDDTest.top10Sessions()
      .map(tuple => TopSession(tuple._1, tuple._2.toString, tuple._3.toString, tuple._4))
    assert(top10Sessions.collect().sameElements(ExpectedResults.top10SessionsExpected))
  }


}

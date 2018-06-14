package com.manojmallela.lastfm

import com.manojmallela.lastfm.common.Utils
import com.manojmallela.lastfm.solutions.{SolveUsingDataFrame, SolveUsingRDD}
import com.manojmallela.lastfm.common.DAO.InputArgs
import com.manojmallela.lastfm.solutions.{SolveUsingDataFrame, SolveUsingRDD}
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object LogAnalyzer extends App {


  override def main(args: Array[String]): Unit = {

    val parser = new OptionParser[InputArgs]("LastFM Log Analytics") {

      opt[String]('m', "master").required().action { (a, c) =>
        c.copy(sparkMaster = a)
      }.text("Spark Master - local[*], yarn, kubernetes (k8s://host:port)")
      opt[Int]('e', "execution-mode").required().action { (e, c) =>
        c.copy(executionMode = e)
      }.text("Execution mode (Int) - 1: RDD or 2:DF or 3:Both")
      opt[String]('i', "inputPath").required().action { (i, c) =>
        c.copy(inputFilePath = i)
      }.text("LastFm data file path")
      opt[String]('o', "outputPath").required().action { (o, c) =>
        c.copy(outputFilePath = o)
      }.text("Path to write results")
    }

    var config: InputArgs = null
    parser.parse(args, InputArgs()) match {
      case Some(inputArgs) => {
        config = inputArgs
      }
      case _ => {
        System.err.println("Invalid arguments")
        System.exit(1)
      }
    }

    val spark = Utils.createSparkSession(appName = "LastFM Analytics", config.sparkMaster)

    config.executionMode match {
      case 1 =>
        runRDD(spark, config)
      case 2 => {
        runDataFrame(spark, config)
      }
      case 3 =>
        runRDD(spark, config)
        runDataFrame(spark, config)
    }

    spark.stop()
  }


  def runRDD(spark: SparkSession, config: InputArgs): Unit = {
    val solveUsingRDD = new SolveUsingRDD(spark, config.inputFilePath)
    solveUsingRDD.init()
    solveUsingRDD.executeTasksABC(s"${config.outputFilePath}/rdd/")
  }

  def runDataFrame(spark: SparkSession, config: InputArgs): Unit = {
    val solveUsingDataFrame = new SolveUsingDataFrame(spark, config.inputFilePath)
    solveUsingDataFrame.init()
    solveUsingDataFrame.executeTasksABC(s"${config.outputFilePath}/df/")
  }

}

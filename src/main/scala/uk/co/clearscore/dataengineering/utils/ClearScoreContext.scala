package uk.co.clearscore.dataengineering.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Provides the necessary Spark Context and SQL context for using the Spark Framework
  */
object ClearScoreContext {

  private var _sqlContext: Option[SparkSession] = None
  private var _sparkContext: Option[SparkContext] = None


  /**
    * Provides an already existing Spark Context or initializes a new instance
    *
    * @return a valid Spark Context for usage
    * @param localContext Defines if the spark Context should be set as standalone for local executions
    */
  def sparkContext(localContext: Boolean = false): SparkContext = {

    val sc = _sparkContext match {
      case Some(someSc) => someSc
      case None => {
        initializeContext(localContext)
      }
    }

    _sparkContext.get

  }

  /**
    * Provides an already existing SQL Context or initializes a new instance
    *
    * @return a valid SQL Context for usage
    * @param localContext Defines if the Spark Context should be set as standalone for local executions
    */
  def sqlContext(localContext: Boolean = false): SparkSession = {

    if (_sqlContext.isEmpty) initializeContext(localContext)

    _sqlContext.get

  }


  /**
    * Initializes a new instance of a Spark Context and SQL Context
    *
    * @param localContext Defines if the Spark Context should be set as standalone for local executions
    */
  def initializeContext(localContext: Boolean = false) = {


    val sparkConf = new SparkConf().setAppName("ClearScore-DataEngineering-Test")

    if (localContext) sparkConf.setMaster("local")
      .set("spark.driver.host", "localhost")

    val sparkContext = new SparkContext(sparkConf)
    //Do not generate _SUCCESS files
    sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    //Do not generate metadata files


    _sparkContext = Some(sparkContext)

    val sqlContext = SparkSession.builder().getOrCreate()

    _sqlContext = Some(sqlContext)

    //Define SparkContext Log Level
    _sparkContext.get.setLogLevel("WARN")


  }


  /**
    * Stops a running instance of a Spark Context
    */
  def stopSparkContext() = {

    if (_sparkContext.isDefined) {
      _sparkContext.get.stop()
      _sparkContext = None
    }


  }

}

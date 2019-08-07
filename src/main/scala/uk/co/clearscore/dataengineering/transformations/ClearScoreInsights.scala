package uk.co.clearscore.dataengineering.transformations

import java.io.File

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import uk.co.clearscore.dataengineering.utils.ClearScoreContext

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Provides functionalities for obtaining data insights over ClearScore Data
  */
object ClearScoreInsights {

  /**
    * Returns the list of data files available in a given directory
    * @return The data files available in the input directory
    * @param basePath The base directory containing all the data available for analysis
    */
  def getDataFiles(basePath: File):Seq[File] = {

    val reportFiles = new ArrayBuffer[File]

    if (basePath.isFile && basePath.getName.endsWith(".json")){
      reportFiles += basePath
    } else if (basePath.isDirectory) {
      reportFiles ++= basePath.listFiles().flatMap(getDataFiles(_))
    }

    reportFiles

  }


  /**
    * Returns the most recent report data for each of the users available in the input data
    *
    * @return The most recent report file by user available in a set of reports
    * @param allReports A full list of reports available for analysis
    */
  def getLatestReportFiles(allReports: Seq[File]):Seq[File] = {

    val reportFiles = new mutable.HashMap[String, File]

    for (el <- allReports) {

      val accountForReport = el.getParentFile.getName
      val reportID = el.getName.replaceAllLiterally(".json","")

      if (reportFiles.get(accountForReport).isEmpty) {

        reportFiles.put(accountForReport, el)
      } else {

        if (reportFiles.get(accountForReport).get.getName.replaceAllLiterally(".json","").toInt < reportID.toInt) {

          reportFiles.put(accountForReport, el)

        }
      }

    }

    reportFiles.values.toSeq

  }


  /**
    * Calculates the average Credit Score through all the reports provided in the input
    *
    * @return The average Credit score for all reports provided as input
    * @param reportsDF DataFrame holding all the reports for average calculation
    */
  def getAverageCreditScore(reportsDF: DataFrame):DataFrame={

    //Average credit score
    val flattenedReportsScoreDF = reportsDF.select(explode(col("report.ScoreBlock.Delphi.Score")).as("score"))

    flattenedReportsScoreDF.select(avg(col("score").cast(DoubleType)).as("avg_score")).select("avg_score")

  }

  /**
    * Provides the total number of users in each employment status
    *
    * @return Number of users by Employment status
    * @param accountsDF DataFrame holding all the accounts information
    */
  def getUsersByEmploymentStatus(accountsDF: DataFrame)={

    val employmentStatusDF = accountsDF.select("account.user.employmentStatus")
        .withColumn("employmentStatusProcessed",coalesce(col("employmentStatus"),lit("N/D")))
      .rdd.map( row => (row.getString(1),1))

    val out = employmentStatusDF.reduceByKey(_+_)

    val outputSchema = StructType(List(new StructField("EmploymentStatus",StringType,false),new StructField("numberOfUsers",IntegerType,false)))

    val outputDF = ClearScoreContext.sqlContext(true).createDataFrame(out.map( entry => Row(entry._1,entry._2)),outputSchema)

    outputDF

  }


  /**
    * Provides the spread of the credit score ranges of users
    *
    * @return Number of users in each credit score range
    * @param reportsDF DataFrame holding all the credit reports data
    * @param rangeSize size of each credit score range
    */
  def getUsersByScoreRange(reportsDF:DataFrame, rangeSize:Int) = {


    def computeRangeIdentifier(rangeID:Long,rangeSize:Int)={

      val bucketIdentifier = rangeID match {

        case 0 => "[0-" + (rangeID+1 * rangeSize) + "]"
        case _ => "[" + (rangeID * rangeSize + 1) + "-" + (rangeID + 1)*rangeSize + "]"

      }

      bucketIdentifier

    }


    val elementsByRange = reportsDF.select(explode(col("report.ScoreBlock.Delphi.Score")).as("score"))
      .withColumn("range", ceil(col("score").cast(DoubleType) / rangeSize)-1)
      //Adjustment necessary for the first bucket with one more element than the rest
      .withColumn("rangeAdjusted",when(col("range") === -1,lit(0)).otherwise(col("range")))
      .rdd.map( row => (row.getLong(2),1))
      .reduceByKey( _ + _)

    val maxValueInRange = elementsByRange.reduce((x,y) => if(x._1 > y._1) x else y)

    val allRanges = for (i <- 0 to maxValueInRange._1.toInt) yield (i.toLong,0)

    val bucketsRDD = ClearScoreContext.sparkContext(true).parallelize(allRanges)

    val allRangesRDD = bucketsRDD.leftOuterJoin(elementsByRange)

    val allRangesSortedRDD = allRangesRDD.sortByKey(true)

    val outputByRange = allRangesSortedRDD.map( x => Row(computeRangeIdentifier(x._1,rangeSize),x._2._2.getOrElse(0) ))

    val outputSchema = StructType(List(new StructField("Range",StringType,false),new StructField("NumberOfElements",IntegerType,false)))

    val outputDF = ClearScoreContext.sqlContext(true).createDataFrame(outputByRange,outputSchema)

    outputDF

  }


  /**
    * Provides enriched Bank Data based on the information available on credit reports and accounts data.
    *
    * @return DataFrame with enriched Bank Data
    * @param accountsDF DataFrame holding all the accounts data for analysis
    * @param reportsDF DataFrame holding all the credit reports data for analysis
    */
  def getEnrichedBankData(accountsDF:DataFrame,reportsDF:DataFrame) = {

    val accountSubsetDF = accountsDF.select(col("uuid").as("user_uuid"),
      col("account.user.employmentStatus").as("employment_Status"),
      col("account.user.bankName").as("bank_Name"))

    val reportsSubsetDF = reportsDF.select(col("user-uuid").as("user_uuid"),
      col("report.Summary.Payment_Profiles.CPA.Bank.Total_number_of_Bank_Active_accounts_").as("number_of_active_accounts").cast(IntegerType),
      col("report.Summary.Payment_Profiles.CPA.Bank.Total_outstanding_balance_on_Bank_active_accounts").as("total_outstanding_balance").cast(DoubleType))

    accountSubsetDF.join(reportsSubsetDF,Seq("user_uuid"),"inner")

  }


}

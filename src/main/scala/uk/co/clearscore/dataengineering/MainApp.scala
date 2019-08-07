package uk.co.clearscore.dataengineering

import java.io.File

import uk.co.clearscore.dataengineering.transformations.ClearScoreInsights
import uk.co.clearscore.dataengineering.utils.{ClearScoreContext, FileUtils}

/**
  * Provides ClearScore Data insights over accounts and credit reports data
  */
object MainApp {


  /**
    * Executes ClearScore Data Insights application
    * @param args Local filesystem locations to accounts data (args[0]), reports data (args[1]) and data insights output (args[2])
    */
  def main(args: Array[String]): Unit = {

    val accountsDataFilesLocation = args(0)
    val reportsDataFilesLocation = args(1)
    val outputLocation = args(2)

    args.foreach(inputPath => {
      if (!new File(inputPath).isDirectory) {
        throw new Exception(s"FileSystem path $inputPath does not exist")
      }
    })

    val accountJSONFiles = ClearScoreInsights.getDataFiles(new File(accountsDataFilesLocation)).map(_.getCanonicalPath)

    val reportsJSONFiles = ClearScoreInsights.getDataFiles(new File(reportsDataFilesLocation)).map(_.getCanonicalPath)

    val accountsDF = ClearScoreContext.sqlContext(true).read.json(accountJSONFiles: _*)

    val reportsDF = ClearScoreContext.sqlContext(true).read.json(reportsJSONFiles: _*)

    val avgScore = ClearScoreInsights.getAverageCreditScore(reportsDF)

    FileUtils.writeDataFrameToCSV(avgScore,outputLocation,"1.csv")

    val usersByEmploymentStatusDF = ClearScoreInsights.getUsersByEmploymentStatus(accountsDF)

    FileUtils.writeDataFrameToCSV(usersByEmploymentStatusDF,outputLocation,"2.csv")

    val allReportFiles = ClearScoreInsights.getDataFiles(new File(reportsDataFilesLocation))

    val latestReports = ClearScoreInsights.getLatestReportFiles(allReportFiles).map(_.getCanonicalPath)

    val latestReportsDF = ClearScoreContext.sqlContext(true).read.json(latestReports: _*)

    val scoreBucketsDF = ClearScoreInsights.getUsersByScoreRange(latestReportsDF,50)

    FileUtils.writeDataFrameToCSV(scoreBucketsDF,outputLocation,"3.csv")

    val enrichedDataDF = ClearScoreInsights.getEnrichedBankData(accountsDF,latestReportsDF)

    FileUtils.writeDataFrameToCSV(enrichedDataDF,outputLocation,"4.csv")

    ClearScoreContext.stopSparkContext()

  }

}

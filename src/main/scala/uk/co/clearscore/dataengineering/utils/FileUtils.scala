package uk.co.clearscore.dataengineering.utils

import java.io.File
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * Provides functionalities to handle reading and Writing CSV files in the Local Filesystem
  */
object FileUtils {

  /**
    * Loads a CSV file in filesystem to a DataFrame
    *
    * @return DataFrame
    * @param path Defines if the spark Context should be set as standalone for local executions
    */
  def loadCSVToDataFrame(path:String):DataFrame = {

    ClearScoreContext.sqlContext().read.format("com.databricks.spark.csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("delimiter",",")
      .load(s"file://${path}")

  }

  /**
    * Writes a DataFrame into the filesystem in CSV format
    *
    * @param df DataFrame to persist
    * @param outputPath Defines the full output path for the CSV file
    * @param outputFileName Defines the filename for the CSV file
    */
  def writeDataFrameToCSV(df:DataFrame,outputPath:String,outputFileName:String) = {

    df.coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header","true")
      .option("delimiter",",")
      .mode(SaveMode.Append)
      .save(outputPath)

    val outputDirectory = new File(outputPath)

    outputDirectory.listFiles().foreach( file => {

      //Removes metadata files being generated
      if (file.getName.endsWith(".crc")) {
        file.delete()
      } else {
        if (file.getName.startsWith("part-")) file.renameTo(new File(s"$outputPath/$outputFileName"))
      }
    })



  }


}

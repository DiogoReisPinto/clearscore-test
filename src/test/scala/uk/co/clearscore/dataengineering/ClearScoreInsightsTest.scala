package uk.co.clearscore.dataengineering

import java.io.File

import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import uk.co.clearscore.dataengineering.transformations.ClearScoreInsights
import uk.co.clearscore.dataengineering.utils.{ClearScoreContext, FileUtils}

class ClearScoreInsightsTest extends FunSuite with BeforeAndAfterAll{

  val basePathForReports = getClass.getResource("/input/reports/").getPath
  val basePathForAccounts = getClass.getResource("/input/accounts/").getPath
  val employmentStatusExpectedOutputLocation = getClass.getResource("/output/employmentStatusExpected.csv").getPath
  val UsersScoreInRangesExpectedOutputLocation = getClass.getResource("/output/UsersScoreInRangesExpected.csv").getPath
  val enrichedBankDataExpectedOutputLocation = getClass.getResource("/output/enrichedBankDataExpected.csv").getPath
  var reportsData:Seq[File] = _
  var accountsData:Seq[File] = _
  var latestReportsData:Seq[File] = _
  var accountsDF:DataFrame = _
  var reportsDF:DataFrame = _
  var latestReportsDF:DataFrame = _

  override def beforeAll() = {

    reportsData = ClearScoreInsights.getDataFiles(new File(basePathForReports)).sorted
    accountsData = ClearScoreInsights.getDataFiles(new File(basePathForAccounts)).sorted
    latestReportsData = ClearScoreInsights.getLatestReportFiles(reportsData).sorted

    accountsDF = ClearScoreContext.sqlContext(true).read.json(accountsData.map(_.getCanonicalPath): _*)
    reportsDF = ClearScoreContext.sqlContext(true).read.json(reportsData.map(_.getCanonicalPath): _*)
    latestReportsDF = ClearScoreContext.sqlContext(true).read.json(latestReportsData.map(_.getCanonicalPath): _*)

  }

  override def afterAll() = {

    ClearScoreContext.stopSparkContext()

  }

  test("Read reports data"){

    val expectedReportsReturned = Seq("1.json","2.json","3.json","4.json","5.json","6.json","7.json").sorted

    assert(expectedReportsReturned.equals(reportsData.map(_.getName)))

  }


  test("Read accounts data"){

    val expectedAccountsReturned = Seq("0f331b0b-f1a2-4e5d-8284-33ca9bf41254.json","1bc25bc6-9a63-4ea7-ab83-fb764399da7b.json","1c609037-24c4-4d16-b0e6-db030039234d.json").sorted

    assert(expectedAccountsReturned.equals(accountsData.map(_.getName)))

  }


  test("Read Latest Reports data"){

    val expectedReportsReturned = Seq("6.json","7.json").sorted

    assert(expectedReportsReturned.equals(latestReportsData.map(_.getName)))

  }


  test("Get Average Credit Score"){

    val averageExpected = 153.85714285714286

    val averageCalculated = ClearScoreInsights.getAverageCreditScore(reportsDF).first().getDouble(0)

    assert(averageCalculated == averageExpected)

  }


  test("Get Number of users grouped by employment Status"){

    val usersByEmploymentStatusDF = ClearScoreInsights.getUsersByEmploymentStatus(accountsDF)

    val expectedUsersByEmploymentStatusDF = FileUtils.loadCSVToDataFrame(employmentStatusExpectedOutputLocation)

    val differenceDF = usersByEmploymentStatusDF.exceptAll(expectedUsersByEmploymentStatusDF)

    assert(differenceDF.count == 0)

  }


  test("Get Number of users in score ranges"){

    val usersInScoreRangesDF = ClearScoreInsights.getUsersByScoreRange(reportsDF,50)

    val expectedUsersInScoreRangesDF = FileUtils.loadCSVToDataFrame(UsersScoreInRangesExpectedOutputLocation)

    val differenceDF = usersInScoreRangesDF.exceptAll(expectedUsersInScoreRangesDF)

    assert(differenceDF.count == 0)

  }


  test("Get Enriched Bank Data"){

    val enrichedBankDataDF = ClearScoreInsights.getEnrichedBankData(accountsDF,latestReportsDF)

    val expectedEnrichedBankDataDF = FileUtils.loadCSVToDataFrame(enrichedBankDataExpectedOutputLocation)

    val differenceDF = enrichedBankDataDF.exceptAll(expectedEnrichedBankDataDF)

    assert(differenceDF.count()==0)

  }


}

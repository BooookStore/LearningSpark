package loadandsave

import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

/**
  * データのロードと保存の基本操作
  */
object BasicOperation extends App {

  val sparkConf = new SparkConf().setSparkHome("local").setAppName("BasicLoadAndSaveOperation")
  val sparkContext = new SparkContext(sparkConf)

  textFile
  csvFile

  /**
    * テキストファイルのロードと保存
    */
  def textFile(): Unit = {

    // テキストファイルのロード
    val salefiles = sparkContext.wholeTextFiles("inputFiles/salesFiles/")

    // それぞれのファイルから、平均値を計算
    val result = salefiles.mapValues { y =>
      val nums = y.split(" ").map(x => x.toDouble)
      nums.sum / nums.size.toDouble
    }

    // テキストファイルの保存
    result.saveAsTextFile("Result/saleDate")
  }

  def csvFile(): Unit = {

    val LOCAL_CODE_INDEX = 2
    val LOCAL_NAME_INDEX = 6
    val LOCAL_MAN_INDEX = 8
    val LOCAL_WOMAN_INDEX = 9
    val YEAR_2015 = 4

    // CSVファイルのロード
    val csvFile = sparkContext.textFile("inputFiles/csvFiles/002_00_utf8.csv")

    // CSVファイルのフォーマット
    val result = csvFile.map { line =>
      val reader = new CSVReader(new StringReader(line))
      reader.readNext
    }

    // CSVから必要のないデータをフィルタ
    val ps = result.filter(line => Try(line(YEAR_2015).toInt).isSuccess && !line(YEAR_2015).isEmpty).persist

    // 地域と、コードのペアRDDの作成
    val localWithCode = ps.map(line => (line(LOCAL_CODE_INDEX), line(LOCAL_NAME_INDEX))).persist
    val manWithCode = ps.map(line => (line(LOCAL_CODE_INDEX), line(LOCAL_MAN_INDEX).toInt))
    val womanWithCode = ps.map(line => (line(LOCAL_CODE_INDEX), line(LOCAL_WOMAN_INDEX).toInt))

    // CSVファイルをテキストとして保存
    localWithCode.saveAsTextFile("Result/localWithCode")
    manWithCode.saveAsTextFile("Result/manWithCode")
    womanWithCode.saveAsTextFile("Result/womanWithCode")
  }

}

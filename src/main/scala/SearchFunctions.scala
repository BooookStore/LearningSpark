import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SearchFunctionsApplication extends App {

  val conf = new SparkConf().setMaster("local").setAppName("SearchFunctionsApplication")
  val sc = new SparkContext(conf)

  val lines = sc.textFile("./SampleTextFile.md")

  val linesC = new SearchFunctions("c").getMatchesReference(lines)
}

/**
  * 検索を行う関数を定義。Sparkに渡すことになるが、その時の注意点を明記した。
  *
  * @param query 検索対象文字列
  */
class SearchFunctions(val query: String) {

  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  def getMatchFunctionReference(rdd: RDD[String]): RDD[Boolean] = {
    // 問題あり
    // query は this.query なので、 this 全体を渡してしまっている。
    rdd.map(isMatch)
  }

  def getMatchesFieldReference(rdd: RDD[String]): RDD[Array[String]] = {
    // 問題あり
    // query は this.query なので、 this 全体を渡してしまっている。
    rdd.map(x => x.split(query))
  }

  def getMatchesReference(rdd: RDD[String]): RDD[Array[String]] = {
    val query_ = query
    rdd.map(x => x.split(query_))
  }

}

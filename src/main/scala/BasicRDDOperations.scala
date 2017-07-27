import org.apache.spark.{SparkConf, SparkContext}

object BasicRDDOperations extends App {

  val conf = new SparkConf().setMaster("local").setAppName("SearchFunctionsApplication")
  val sc = new SparkContext(conf)

  // RDD内の整数を二乗する。
  val inputs = sc.parallelize(List(1, 2, 3, 4))
  val result = inputs.map(x => x * x)
  println(result.collect().mkString(","))

  // 行を複数の単語に分割する。
  val lines = sc.parallelize(List("Hello World", "hi"))
  val words = lines.flatMap(line => line.split(" "))
  println(words.first()) // Hello を返す。

  // ２つのRDDのカルテシアン積を求める。
  val users = sc.parallelize(List("Tom", "Bob", "Alex"))
  val venues = sc.parallelize(List("Betabrand", "Asha Tea House", "Ritual"))
  val pairs = users.cartesian(venues)
  pairs.foreach(p => println(s"cartesian[$p]"))

}

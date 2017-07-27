import org.apache.spark.{SparkConf, SparkContext}

/**
  * 基本的なアクション
  */
object BasicRDDActions extends App {
  val conf = new SparkConf().setMaster("local").setAppName("BasicRDDActions")
  val sc = new SparkContext(conf)

  // mapの使い方
  val inputs = sc.parallelize(List(1, 2, 3, 4))
  val sum = inputs.reduce((x, y) => x + y)
  println(s"sum[$sum]")

  // foldの使い方
  val product = inputs.fold(1)((l, r) => l * r)
  println(s"product[$product]")

  // aggregateの使い方
  val result = inputs.aggregate((0, 0))(
    (acc, value) => (acc._1 + value, acc._2 + 1),
    (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
  val avg = result._1 / result._2.toDouble
  println(s"avg[$avg]")
}

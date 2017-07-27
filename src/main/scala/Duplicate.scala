import org.apache.spark.{SparkConf, SparkContext}

object Duplicate extends App {
  val conf = new SparkConf().setMaster("local").setAppName("SearchFunctionsApplication")
  val sc = new SparkContext(conf)

  val inputs = sc.parallelize(List(1,2,3,4))
  val result = inputs.map(x => x * x)
  println(result.collect().mkString(","))
}

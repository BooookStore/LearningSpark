import org.apache.spark.{SparkConf, SparkContext}

/**
  * キー・値ペアの処理
  */
object KeyValue extends App {

  val sparkConf = new SparkConf().setSparkHome("local").setAppName("KeyValue")
  val sparkContext = new SparkContext(sparkConf)

  val lines = sparkContext.textFile("./SampleTextFile.md")

  // キー・値ペアのRDDを生成するにはタプル群を返す必要がある。
  // タプル軍からなるRDDに対しては、暗黙の変換がある。
  val pairs = lines.map(x => (x.split(" ")(0), x))

  // 20文字以上の行をフィルタリングより取り除く
  val filterd = pairs.filter{case (key,value) => value.length < 20}

  filterd.foreach(p => println(s"[$p]"))
}

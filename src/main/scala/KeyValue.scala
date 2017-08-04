import org.apache.spark.SparkContext.{rddToOrderedRDDFunctions, rddToPairRDDFunctions}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * キー・値ペアのRDDに対するサンプルコード
  */
object KeyValue extends App {

  val sparkConf = new SparkConf().setSparkHome("local").setAppName("KeyValue")
  val sparkContext = new SparkContext(sparkConf)

  basicKeyValueOperation(sparkContext)
  collection(sparkContext)
  union(sparkContext)
  sort(sparkContext)

  /**
    * 基本的なキー・値ペアの処理
    *
    * @param sparkContext
    */
  def basicKeyValueOperation(sparkContext: SparkContext): Unit = {
    val lines = sparkContext.textFile("./SampleTextFile.md")

    // キー・値ペアのRDDを生成するにはタプル群を返す必要がある。
    // タプル軍からなるRDDに対しては、暗黙の変換がある。
    val pairs = lines.map(x => (x.split(" ")(0), x))

    // 20文字以上の行をフィルタリングより取り除く
    val filterd = pairs.filter { case (key, value) => value.length > 20 }

    filterd.foreach(p => println(s"\t$p"))
  }

  /**
    * 集計処理の例
    *
    * @param sparkContext
    */
  def collection(sparkContext: SparkContext): Unit = {

    // データセットを用意
    val animal = sparkContext.parallelize(List(
      ("panda", 0),
      ("pink", 3),
      ("pirate", 3),
      ("panda", 1),
      ("pink", 4)
    ))

    val lines = sparkContext.textFile("SampleTextFile.md")

    val drink = sparkContext.parallelize(List(
      ("coffee", 1),
      ("coffee", 2),
      ("panda", 3),
      ("coffee", 9)
    ), 2)

    // reduceByKey()はreduce()とよく似ているが、各キーとそのキーに対するreduceされた値からなる新しいRDDを返す
    // 以下のコードは各キーに対する平均値を集計している
    val collected = animal.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    println("# reduceByKeyを利用した集計処理")
    collected.foreach(v => {
      val key = v._1
      val value = v._2
      println(s"\t[$key : $value]")
    })

    // reduceByKey()とflatMap()を組み合わせることで古典的なワードカウントを実装
    val words = lines.flatMap(x => x.split(" ")).map(x => (x, 1)).reduceByKey((x, y) => x + y)

    println("# reduceByKeyを利用したワードカウント処理")
    words.foreach(v => println(s"\t[${v._1} : ${v._2}]"))

    // combineByKey()を使ってキーごとの平均とを計算
    val result = drink.combineByKey(
      (v) => (v, 1), // 初めて見るキーの場合アキュムレータの初期値を生成
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), // すでに見たキーの場合アキュムレータを更新
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2) // 複数のパーティションのアキュムレータを結合
    ).map { case (key, value) => (key, value._1 / value._2.toFloat) }

    println("# combineByKeyを利用した平均値の計算")
    result.collectAsMap().foreach(println(_))

  }

  /**
    * 結合の例
    *
    * @param sparkContext
    */
  def union(sparkContext: SparkContext) = {

    // データセットを用意
    val storeAddress = sparkContext.parallelize(List(
      ("Ritual", "1026 Valencia St"),
      ("Philz", "748 Ban Ness Ave"),
      ("Philz", "3101 24th St"),
      ("Starbucks", "seattle")
    ))

    val storeRating = sparkContext.parallelize(List(
      ("Ritual", 4.9),
      ("Philz", 4.8)
    ))

    // join()を使用し、内部結合を行う
    // 双方のペアRDDに含まれるキーだけが出力される
    // 同じキーに対して複数の値がある場合には、結果のペアRDDには２つの入力RDDのそのキーで可能なすべての値の組み合わせに対してエントリが作られる
    val joined = storeAddress.join(storeRating)

    println("# joinによる内部結合")
    joined.foreach(j => println(s"\t[${j._1} -> ${j._2._1}(${j._2._2})]"))

    val leftJoined = storeAddress.leftOuterJoin(storeRating)

    println("# lefOuterJoinによる外部結合")
    leftJoined.foreach(j => println(s"\t[${j._1} -> ${j._2._1}(${j._2._2})]"))
  }

  /**
    * ソートの例
    *
    * @param sparkContext
    */
  def sort(sparkContext: SparkContext) = {
    val alphabet = sparkContext.parallelize('A' to 'Z').map(x => (x, x))

    // 要素の順序を決定する
    // 暗黙的に使用されるため、必ずimplicitで宣言する必要あり
    implicit val sortIntegersByString = new Ordering[String] {
      override def compare(a: String, b: String) = a.compare(b)
    }
    val sorted = alphabet.sortByKey();

    println("# sortByKeyによるデータのソート")
    sorted.collect().take(10).foreach(a => println(s"(${a._1},${a._2})"))
  }
}

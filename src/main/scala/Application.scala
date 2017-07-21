import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by bookstore on 17/07/19.
  */
object Application extends App {

  // SparkContextの生成
  val conf = new SparkConf()
    // クラスタのURLを指定。localは特殊な値であり、Sparkをクラスタへ接続することなくローカルマシンの単一スレッドで動作させる。
    .setMaster("local")
    // アプリケーション名。クラスタマネージャのUIではここで指定した名前でアプリケーションを識別する。
    .setAppName("My App")
  val sc = new SparkContext(conf)

  // あらゆるSparkのプログラムやシェルのセッションは大まかに次のように動作する。

  // 1.外部のデータから何らかの入力RDDを生成。
  val lines = sc.textFile("./README.md")

  // 2.RDDを別のRDDへ変換
  val pythonLines = lines.filter(_.contains("python"))

  // 3.永続化したい中間的なRDDがあればメモリへ永続化する
  pythonLines.persist()

  // 4.アクションを呼び並列演算を実行し、結果を取得する
  val count = pythonLines.count()

  // SparkContextをストップ
  // 最後に呼ばないと、例外が発生
  sc.stop()
}

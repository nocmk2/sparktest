import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mk on 16/8/30.
  */


object testCheckpoint {
  def main(args: Array[String]): Unit = {
    val sFile = "/Users/mk/demo.csv"
    val conf = new SparkConf().setAppName("SA")
          .setMaster("local") //-Dspark.master=local

    val sc = new SparkContext(conf)
    val rdd = sc.textFile(sFile).cache()
    println(rdd.filter(x=>x.contains("a")).count())
    println(rdd.filter(x=>x.contains("b")).count())
  }

}

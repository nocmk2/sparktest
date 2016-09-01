import org.apache.spark.sql.SparkSession


/**
  * Created by mk on 16/8/31.
  */
object CreatingDataFrames {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.json("/Users/mk/Documents/spark-2.0.0-bin-hadoop2.7/examples/src/main/resources/people.json")

    df.printSchema()
    //df.select($"name",$"age"+1).show()
    df.filter($"age">21).show()
    df.groupBy("age").count().show()
    //df.select("name").show()
    // Displays the content of the DataFrame to stdout
    //df.show()

    df.createOrReplaceTempView("people")

    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()
  }
}

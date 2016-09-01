// $example on:schema_inferring$
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
// $example off:schema_inferring$
import org.apache.spark.sql.Row
// $example on:init_session$
import org.apache.spark.sql.SparkSession
// $example off:init_session$
// $example on:programmatic_schema$
// $example on:data_types$
import org.apache.spark.sql.types._
// $example off:data_types$
// $example off:programmatic_schema$
/**
  * Created by mk on 16/8/31.
  */
object CreatingDatasets {
  case class Person(name: String, age: Long)


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
    // you can use custom classes that implement the Product interface


    // Encoders are created for case classes
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()
    // +----+---+
    // |name|age|
    // +----+---+
    // |Andy| 32|
    // +----+---+

    // Encoders for most common types are automatically provided by importing spark.implicits._
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)

    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    val path = "/Users/mk/Documents/spark-2.0.0-bin-hadoop2.7/examples/src/main/resources/people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()



  }

}

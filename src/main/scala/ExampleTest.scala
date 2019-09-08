import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ExampleTest extends App {

    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    val spark = SparkSession.builder()
      .appName("streamingApp")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val df: DataFrame = Seq((1, "IL", "USA"), (2, "IL", "USA"), (3, "MO", "USA"), (4, "IL", "USA"), (5, "KA", "INDIA"), (6, "MEL", "AUS")
    ).toDF("id", "state", "country")

    val df3: DataFrame = Seq((1, "IL","USA"), (2, "IL", "USA"), (3, "MO", "USA"), (4, "IL", "USA"), (5, "KA", "INDIA"), (6, "MEL", "AUS")
    ).toDF("id", "state", "country")

    val deltaPath = "src/main/resources/delta/myDeltaTable"
    val parquetPath = "src/main/resources/parquet/myParquet"
    val jsonPath = "src/main/resources/json/sample1"
    println("writing in delta format..")
    df3
      .write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .save(deltaPath)

    println("writing in parquet..")
    df3
      .write
      .format("parquet")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .save(parquetPath)

    println("writing in text..")
    df3.write
        .format("json")
        .option("overwriteSchema",true)
        .save(jsonPath)

    //written n delta format
    spark.read.format("delta").load(deltaPath).show()
    spark.read.format("parquet").load(deltaPath).show()

    //written parquet
    spark.read.format("delta").load(parquetPath).show()
    spark.read.format("parquet").load(parquetPath).show()

    //written in json
    spark.read.format("json").load(jsonPath).show()

    spark.stop()
}

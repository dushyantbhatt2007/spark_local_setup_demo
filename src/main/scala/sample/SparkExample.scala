package sample

import org.apache.spark.sql.SparkSession

object SparkExample {

  private val ENVIRONMENT_PROD = "prod"
  private val ENVIRONMENT_DEV = "dev"
  private val ENVIRONMENT_TEST = "test"

  val deltaTablePath = "./src/main/resources/data/SampleTable"

  def main(args: Array[String]) {

    val spark = getSparkSession("test", "SparkExample")

    // Define the list of dictionaries
    val data = Seq(
      ("Jane", 25, "San Francisco"),
      ("John", 30, "New York"),
      ("Mike", 35, "Boston")
    )

    // Convert the list of dictionaries to DataFrame
    val df = spark.createDataFrame(data).toDF("name", "age", "city")

    // Write the DataFrame as a Delta table
    df.write.mode("overwrite").format("delta").save(deltaTablePath)

    // Read from Delta Table
    val deltaTableDF = spark.read.format("delta").load(deltaTablePath)
    deltaTableDF.show()

    spark.stop()
  }

  private def getSparkSession(environment: String, appName: String): SparkSession = {
    val spark = if (environment == ENVIRONMENT_TEST) {
      SparkSession.builder()
        .appName(appName)
        .master("local")
        .config("spark.ui.port", "40151")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    } else {
      SparkSession.builder()
        .appName(appName)
        .getOrCreate()
    }
    spark
  }
}

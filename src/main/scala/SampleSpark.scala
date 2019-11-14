import org.apache.spark.sql.{Dataset, SparkSession}

object SampleSpark {


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("sample")
      .master("local[*]")
      .getOrCreate()

    val textFile: Dataset[String] = spark.read.textFile("README.md")
    val maxValue: Int = textFile.map(line => line.split(" ").length).reduce((a, b) => Math.max(a, b))
    println(maxValue)
  }

}

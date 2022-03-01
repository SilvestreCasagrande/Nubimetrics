import org.apache.spark.sql.SparkSession

trait Session {
  val spark = SparkSession
    .builder()
    .appName("DataFrame for Fun")
    .master("local")
    .getOrCreate()

  spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
  spark.sparkContext.setLogLevel("ERROR")
}

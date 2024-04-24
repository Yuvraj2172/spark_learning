import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world!")
    val spark = SparkSession.builder().master("local").appName("SparkLearning").getOrCreate()
    val logger = LoggerFactory.getLogger("Spark_Learning")
    logger.info("Program Started")
    println(spark)
    logger.info("Program Finished")
  }
}
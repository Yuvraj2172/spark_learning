package SparkSqlFunctions

import Constants.Constants._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, row_number}


object WindowFunctions {
  def main(args: Array[String]): Unit = {
    logger.info("Program Started")

    import spark.implicits._
    val simpleData = Seq(
      ("Jamaes", "Sales", 3000), ("Michael", "Sales", 4600), ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000), ("James", "Sales", 3000), ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900), ("Jeff", "Marketing", 3000), ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)
    )
    val columns = Seq("employee_name", "department", "salary")
    val df = simpleData.toDF(columns: _*)
    df.show(false)

    val windowSpec = Window.partitionBy("department").orderBy(asc("salary"))

    df.withColumn("row_number", row_number.over(windowSpec))
      .withColumn("rank", rank.over(windowSpec))
      .withColumn("dense_rank", dense_rank().over(windowSpec))
      .withColumn("percent_rank", percent_rank().over(windowSpec))
      .withColumn("ntile_rank", ntile(3).over(windowSpec))
      .withColumn("cume_dist", cume_dist().over(windowSpec))
      .withColumn("lag", lag("salary", 2).over(windowSpec))
      .withColumn("lead", lead("salary", 2).over(windowSpec))
      .show(false)

    val windowSpec2 = Window.partitionBy("department").orderBy("salary")
    val windowSpecAgg = Window.partitionBy("department")

    val aggDf = df.withColumn("row", row_number().over(windowSpec2))
      .withColumn("sum", sum(col("salary")).over(windowSpecAgg))
      .withColumn("avg", avg("salary").over(windowSpecAgg))
      .withColumn("min", min("salary").over(windowSpecAgg))
      .withColumn("max", max("salary").over(windowSpecAgg))
      .where(col("row")===1)
      .select("department", "salary", "sum","avg", "min", "max")
      .show(false)


    logger.info("Program Finished")
  }

}

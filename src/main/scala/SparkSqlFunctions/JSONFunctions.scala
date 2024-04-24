package SparkSqlFunctions

import Constants.Constants._
import org.apache.spark.sql.functions.{col, from_json, get_json_object, json_tuple, lit, schema_of_json, to_json}
import org.apache.spark.sql.types.{MapType, StringType}

object JSONFunctions {
  def main(args: Array[String]): Unit = {
    logger.info("Program Started")

    val jsonString = """{"Zipcode" : 704, "ZipCodeType" : "STANDARD", "City" : "PARC PARQUE", "State" : "PR"}"""
    val data = Seq((1,jsonString))

    import spark.implicits._
    val df = data.toDF("id", "value")
    df.show(false)

    val df2 = df.withColumn("value", from_json(col("value"), MapType(StringType, StringType)))
    df2.printSchema()
    df2.show(false)

    val df3 = df2.withColumn("value", to_json(col("value")))
    df3.show(false)

    df.select( col("id"),json_tuple(col("value"), "Zipcode", "ZipCodeType", "City"))
      .toDF("id","Zipcode", "ZipCodeType", "City")
      .show(false)

    df.select(col("id"), get_json_object(col("value"), "$.Zipcode").as("ZipCode"))
      .show(false)

    val schemaStr = spark.range(1)
      .select(schema_of_json(lit(
        jsonString
      ))).collect()(0)(0)
    println(schemaStr)

    logger.info("Program Finished")
  }

}

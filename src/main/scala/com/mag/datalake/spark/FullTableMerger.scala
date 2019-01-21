package com.mag.datalake.spark

import org.apache.spark.sql.{SparkSession, functions}

object FullTableMerger {

  /*

      To run this program use the below command.

      SYNTAX
      ========

      spark2-submit --class com.mag.datalake.spark.DistTableMerger \
      "JDBC URL" \
      "UserName" \
      "Password" \
      "Source JDBC Table" \
      "Source Hive Table for comparison" \
      "Staging or Target Hive Table" \
      "Business Key Columns ',' separated "

      EXAMPLE
      =========

      spark2-submit --class com.mag.datalake.spark.FullTableMerger --master yarn --deploy-mode cluster --driver-memory 16G --executor-memory 8G SparkTableMerger-assembly-1.0.jar \
      "jdbc:oracle:thin:@//172.25.73.74:1521/MOBILE.RTADB4" \
      "myusername" \
      "mypwd" \
      "EXTERNAL.veh_stop" \
      "pta_avm_raw.veh_stop" \
      "pta_avm_stg.veh_stop" \
      "event_no,opd_date"

   */

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("FullTableMerger")
      .enableHiveSupport()
      .getOrCreate()

    val jdbcURL = args(0)

    val userName = args(1)

    val password = args(2)

    val tableName = args(3)

    val hiveTableName = args(4)

    val stgHiveTableName = args(5)

    val jdbcDriver = jdbcURL match {

      case source if source.contains("oracle") => "oracle.jdbc.driver.OracleDriver"
      case source if source.contains("mysql") => "com.mysql.jdbc.Driver"
      case source if source.contains("sqlserver") => "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      case source if source.contains("postgresql") =>   "org.postgresql.Driver"
    }

    val properties = new java.util.Properties()
    properties.put("user", userName)
    properties.put("password", password)
    properties.put("driver", jdbcDriver)
    properties.put("dbtable", tableName)

    val jdbcDF = spark.read.jdbc(jdbcURL, tableName, properties)

    jdbcDF.cache()

    val hiveQuery = s"""SELECT * from $hiveTableName""".stripMargin

    val hiveDF = spark.sql(hiveQuery)

    val colName = for (c <- hiveDF.columns) yield c + "1"

    val hiveDFRenamed = hiveDF.toDF(colName : _*).withColumn("du33yz", org.apache.spark.sql.functions.lit(""))

    hiveDFRenamed.cache()

    val joinExprs = jdbcDF.columns.zip(hiveDFRenamed.columns)
      .map { case (c1, c2) => jdbcDF(c1) <=> hiveDFRenamed(c2) }.reduce(_ && _)

    jdbcDF.join(hiveDFRenamed, joinExprs, "left").filter("du33yz is null")
      .select(jdbcDF.columns.head, jdbcDF.columns.tail: _*).write.insertInto(stgHiveTableName)

    spark.close()

  }

}
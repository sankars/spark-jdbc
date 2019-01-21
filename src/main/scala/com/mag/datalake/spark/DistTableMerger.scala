package com.mag.datalake.spark

import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, Days}

object DistTableMerger {

  /*

      To run this program use the below command.

      SYNTAX
      ========

      spark2-submit --class com.mag.datalake.spark.DistTableMerger \
      "JDBC URL" \
      "Source Table Name" \
      "UserName" \
      "Password" \
      "CDC Date Column" \
      "Start Date" \
      "End Date" \
      "Raw Hive Table for comparison" \
      "Business Key Columns ',' separated " \
      "Target Hive Table" \
      "No of days of data to be processed per task"

      EXAMPLE
      =========

      spark2-submit --class com.mag.datalake.spark.DistTableMerger --master yarn --deploy-mode cluster --driver-memory 16G --executor-memory 8G SparkTableMerger-assembly-1.0.jar \
      "jdbc:oracle:thin:@//172.25.73.74:1521/MOBILE.RTADB4" \
      "EXTERNAL.veh_stop" \
      "external" \
      "stat" \
      "OPD_DATE" \
      "2018-10-01" \
      "2019-01-13" \
      "pta_avm_raw.veh_stop" \
      "event_no,opd_date" \
      "pta_avm_stg.veh_stop" \
      2

   */

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DistTableMerger")
      .enableHiveSupport()
      .getOrCreate()

    val jdbcURL = args(0)

    val tableName = args(1)

    val userName = args(2)

    val password = args(3)

    val dateColumnName = args(4)

    val startDate = args(5)

    val endDate = args(6)

    val hiveTableName = args(7)

    val joinColumns = args(8)

    val stgHiveTableName = args(9)

    val daysPerTask = args(10).toInt

    val totalDays = Days.daysBetween(DateTime.parse(startDate).toLocalDate(), DateTime.parse(endDate).toLocalDate()).getDays()

    val predicates = for (n <- 1 to (totalDays / daysPerTask)) yield s"""$dateColumnName >= TO_DATE('$startDate', 'YYYY-MM-DD') + ${(n - 1) * daysPerTask}  AND $dateColumnName <  TO_DATE('$startDate', 'YYYY-MM-DD') + ${math.min(totalDays, n * daysPerTask)}"""

    val jdbcDriver = jdbcURL match {

      case source if source.contains("oracle") => "oracle.jdbc.driver.OracleDriver"
      case source if source.contains("mysql") => "com.mysql.jdbc.Driver"
      case source if source.contains("sqlserver") => "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      case source if source.contains("postgresql") => "org.postgresql.Driver"
      case source if source.contains("db2") => "com.ibm.db2.jcc.DB2Driver"
    }

    val properties = new java.util.Properties()
    properties.put("user", userName)
    properties.put("password", password)
    properties.put("driver", jdbcDriver)
    properties.put("dbtable", tableName)

    val jdbcDF = spark.read.jdbc(jdbcURL, tableName, predicates.toArray, properties)

    jdbcDF.cache()

    val hiveQuery =
      s"""SELECT $joinColumns , "" AS du33yz from $hiveTableName where $dateColumnName between CAST("$startDate" as timestamp) and CAST("$endDate" as timestamp)""".stripMargin
    println(s"hiveQuery : $hiveQuery")

    val hiveDF = spark.sql(hiveQuery)

    hiveDF.cache()

    jdbcDF.join(hiveDF, joinColumns.split(",").toSeq, "left").filter("du33yz is null")
      .drop("du33yz").select(jdbcDF.columns.head, jdbcDF.columns.tail: _*).write.insertInto(stgHiveTableName)

    spark.close()

  }

}
package com.mag.datalake.spark

import org.apache.spark.sql.SparkSession

object TableMerger {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("TableMerger")
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

    val whereClause = jdbcURL match {

      case source if source.contains("oracle") =>
        s"""$dateColumnName between TO_DATE('$startDate','yyyy-mm-dd hh24:mi:ss')
          and TO_DATE('$endDate','yyyy-mm-dd hh24:mi:ss')"""
      case _ => s"""$dateColumnName between $startDate and $endDate"""

    }

    val jdbcQuery = s"""(SELECT * from $tableName where $whereClause) tbl"""

    println(s"jdbcQuery : $jdbcQuery")

    val jdbcDF = spark.read.format("jdbc")
      .option("url", jdbcURL)
      .option("user", userName)
      .option("password",password)
      .option("dbtable",jdbcQuery)
      .option("driver","oracle.jdbc.driver.OracleDriver")
      .load()

    jdbcDF.take(5).foreach(println)

    val hiveQuery =
      s"""SELECT $joinColumns , "" AS dummyz from $hiveTableName where $dateColumnName between CAST("$startDate" as timestamp) and CAST("$endDate" as timestamp)""".stripMargin
    println(s"hiveQuery : $hiveQuery")

    val hiveDF = spark.sql(hiveQuery)

    jdbcDF.take(5).foreach(println)


    jdbcDF.join(hiveDF,joinColumns.split(",").toSeq ,"left").filter("dummyz is null")
      .drop("dummyz").select(jdbcDF.columns.head, jdbcDF.columns.tail: _*).write.insertInto(stgHiveTableName)

    spark.close()

  }

}
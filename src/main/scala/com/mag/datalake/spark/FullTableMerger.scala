package com.mag.datalake.spark

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.util.Try

object FullTableMerger {

  /*

      To run this program use the below command.

      SYNTAX
      ========

      spark2-submit --class com.mag.datalake.spark.FullTableMerger \
      "JDBC URL" \
      "UserName" \
      "Password" \
      "Source JDBC Table" \
      "Source Hive Table for comparison" \
      "Staging or Target Hive Table" \
      "Partition Column" \
      "Max Partitions" [OPTIONAL] \
      "Mode" [OPTIONAL]

      EXAMPLE
      =========

      spark2-submit --class com.mag.datalake.spark.FullTableMerger --master yarn --deploy-mode cluster --driver-memory 16G --executor-memory 8G SparkTableMerger-assembly-1.0.jar \
      "jdbc:oracle:thin:@//172.25.73.74:1521/MOBILE.RTADB4" \
      "myusername" \
      "mypwd" \
      "EXTERNAL.veh_stop" \
      "pta_avm_raw.veh_stop" \
      "pta_avm_stg.veh_stop" \
      "event_no" \
      10 \
      "dry-run"

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

    val partitionColumn = args(6)

    val (maxPartitions, mode) = args.length match {

      case 8 => {

        val p = if (Try(args(7).toInt).isSuccess) args(7).toInt else 50
        val m = if (Try(args(7).toInt).isFailure) args(7).toLowerCase() else "cdc"
        (p, m)

      }

      case 9 => {

        val p = args(7).toInt
        val m = args(8).toLowerCase()
        (p, m)

      }
    }


    val samplingThreshold = 1000000

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
    properties.put("fetchsize", "1000")

    val cdcQuery = s"""SELECT * FROM $hiveTableName""".stripMargin

    val dryRunQuery = s"""SELECT $partitionColumn FROM $tableName LIMIT $samplingThreshold""".stripMargin

    val samplingDF = mode match {
      case "cdc" => spark.sql(cdcQuery).cache()
      case "dry-run" => spark.read.jdbc(jdbcURL, s"""($dryRunQuery) tbl""", properties).cache()
      case _ => println("Doing CDC"); spark.sql(cdcQuery).cache();

    }

    val colName = for (c <- samplingDF.columns) yield c + "1"

    val rowCount = samplingDF.count()

    val partitionCount = getPartitionCount(rowCount, maxPartitions)

    val rangeDF = if (rowCount < samplingThreshold) samplingDF.select(partitionColumn) else samplingDF.sample(false, 0.05).select(partitionColumn)

    val partitionRanges = getPartitionRanges(rangeDF, partitionColumn, partitionCount)

    val jdbcSchema = spark.read.jdbc(jdbcURL, tableName, properties)

    val predicates = getPredicates(jdbcURL, jdbcSchema.toDF(), partitionColumn, partitionRanges)

    val jdbcDF = spark.read.jdbc(jdbcURL, tableName, predicates, properties).cache()

    if (mode == "cdc") {

      val hiveDFRenamed = samplingDF.toDF(colName: _*).withColumn("du33yz", org.apache.spark.sql.functions.lit(""))

      val joinExprs = jdbcDF.columns.zip(hiveDFRenamed.columns)
        .map { case (c1, c2) => jdbcDF(c1) <=> hiveDFRenamed(c2) }.reduce(_ && _)

      jdbcDF.join(hiveDFRenamed, joinExprs, "left").filter("du33yz is null")
        .select(jdbcDF.columns.head, jdbcDF.columns.tail: _*).write.insertInto(stgHiveTableName)
    }

    else if (mode == "dry-run") {

      jdbcDF.write.insertInto(stgHiveTableName)
    }

    spark.close()

  }


  def getPartitionCount(totalRows: Long, maxPartitions: Int): Int = {

    val rowsPerConnection = 200000

    totalRows match {

      case x if x < 50000 => 1
      case x if x > 50000 && x <= rowsPerConnection => 2
      case x if x > rowsPerConnection => scala.math.min(maxPartitions, scala.math.ceil(totalRows.toFloat / rowsPerConnection).toInt)

    }
  }


  def getPartitionRanges(df: DataFrame, partitionColumn: String, partitionCount: Int): List[String] = {

    val rowCount = df.count()

    val rowsPerPartition = rowCount / partitionCount

    val rowNos = (rowsPerPartition to (rowCount - rowsPerPartition) by rowsPerPartition).toList

    val colDF = df.withColumn("row_no", functions.row_number().over(Window.orderBy(partitionColumn))).cache()

    colDF.filter(functions.col("row_no").isin(rowNos: _*)).select(partitionColumn).collect().map(_ (0).toString).toList

  }


  def getPredicates(jdbcURL: String, df: DataFrame, partitionColumn: String, partitionRanges: List[String]): Array[String] = {

    val colType = df.schema.fields(df.schema.fieldNames.map(_.toLowerCase).indexOf(partitionColumn.toLowerCase)).dataType

    colType match {

      case _: ShortType | _: IntegerType | _: LongType | _: DoubleType | _: FloatType | _: DecimalType => {

        val l = partitionRanges.toSet.toList
        var r = s"""$partitionColumn < ${l.head}""" :: List[String]()
        r = s"""$partitionColumn >= ${l.last}""" :: r
        r = s"""$partitionColumn is NULL""" :: r

        if (l.size > 1) {

          for (List(left, right) <- l.sliding(2)) {
            r = s"""$partitionColumn >= $left AND $partitionColumn < $right""" :: r
          }
        }

        r.toArray
      }

      case _: StringType | _: VarcharType => {

        val l = partitionRanges.toSet.toList
        var r = s"""$partitionColumn < '${l.head}'""" :: List[String]()
        r = s"""$partitionColumn >= '${l.last}'""" :: r
        r = s"""$partitionColumn is NULL""" :: r

        if (l.size > 1) {

          for (List(left, right) <- l.sliding(2)) {
            r = s"""$partitionColumn >= '$left' AND $partitionColumn < '$right'""" :: r
          }
        }
        r.toArray
      }

    }

  }

}
package com.amway.dms

import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class RptDriver(mode: Int, freq: Int, data_in: String, data_out: String) {
  val query_type = if (freq == 0) "month" else "quarter"

  def init() = {
    val startTime = LocalDateTime.now().toString
    println(s"=== dms report processing starts at $startTime")

    val spark = SparkSession.builder.appName("Ora_to_Maprfs")
      .config("spark.master", "local").getOrCreate()
    val ora_df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(data_in)

    process(ora_df)
    spark.stop()
    val endTime = LocalDateTime.now().toString
    println(s"=== dms report processing ended at $endTime")

  }

  def process(df: DataFrame) = {
    println(s"=== processing ${query_type}ly reports ...")
    val df1 = df.withColumn("TRX_DT", regexp_replace(col("TRX_DT"), "(\\s.+$)", ""))
    val dfm = if (freq == 0) df1.withColumn(query_type, month(to_date(col("TRX_DT"), "M/d/y")))
    else df1.withColumn(query_type, quarter(to_date(col("TRX_DT"), "M/d/y")))
    val dfma = dfm.na.fill(0, Seq(query_type))
      .groupBy(query_type, "TRX_ISO_CNTRY_CD", "DIST_ID", "BAL_TYPE_ID")
      .agg(expr("sum(bal_amt) as TOTAL"))
      .withColumn("TOTAL", regexp_replace(format_number(col("TOTAL"), 2), ",", ""))
      .orderBy(query_type, "TRX_ISO_CNTRY_CD", "BAL_TYPE_ID", "DIST_ID", "TOTAL")
    //    dfma.show()
    val freq_list = dfma.select(query_type).distinct().rdd.map(r => r(0).toString).collect()

    freq_list.foreach(m => println(s"=== $query_type $m"))
    freq_list.foreach(filterByFreq(dfma, _))
  }

  def filterByFreq(df: DataFrame, mq: String): Unit = {
    val m_df = df.filter(query_type + " == " + mq).drop(query_type)
    writeToCsv(m_df, query_type + "-" + String.format("%2s", mq).replace(' ', '0'))
  }

  def writeToCsv(df: DataFrame, prefix: String) = {
    val fn = data_out + "/DMS_" + prefix.toUpperCase + "_" +
      LocalDateTime.now().format(DateTimeFormatter.ofPattern("MM.dd.yyyy-HHmmss.SSS"))
    df
      .repartition(1)
      .write.format("csv")
      .option("header", "true")
      //      .option("quote", "")
      .mode("overwrite")
      .save(fn)
  }

  def rangex(str: String): Seq[Int] =
    str split ",\\s*" flatMap { (s) =>
      val r = """(-?\d+)(?:-(-?\d+))?""".r
      val r(a, b) = s
      if (b == null) Seq(a.toInt) else a.toInt to b.toInt
    }

}

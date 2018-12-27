package com.amway.dms

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.log4j.{Level, Logger}

class RptDriver(data_in: String, data_out: String, mode: String, freq: String, range_seq: Seq[Int], prefix: String) {
  @transient lazy val logger = Logger.getLogger(getClass.getName)

  val query_type = freq

  def init() = {
    val startTime = LocalDateTime.now().toString
    logger.info(s"=== dms report processing starts at $startTime")

    val spark = SparkSession.builder.appName("Ora_to_Maprfs")
      .config("spark.master", "local").getOrCreate()
    val ora_df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(data_in)

    process(ora_df)
    spark.stop()
    val endTime = LocalDateTime.now().toString
    logger.info(s"=== dms report processing ended at $endTime")

  }

  def process(df: DataFrame) = {
    logger.info(s"=== processing ${query_type}ly reports ...")
    val df1 = df.withColumn("TRX_DT", regexp_replace(col("TRX_DT"), "(\\s.+$)", ""))
    val dfm = if (freq == Constant.MM) df1.withColumn(query_type, month(to_date(col("TRX_DT"), "M/d/y")))
    else df1.withColumn(query_type, quarter(to_date(col("TRX_DT"), "M/d/y")))
    val dfma = dfm.na.fill(0, Seq(query_type))
      .groupBy(query_type, "TRX_ISO_CNTRY_CD", "DIST_ID", "BAL_TYPE_ID")
      .agg(expr("sum(bal_amt) as TOTAL"))
      .withColumn("TOTAL", regexp_replace(format_number(col("TOTAL"), 2), ",", ""))
      .orderBy(query_type, "TRX_ISO_CNTRY_CD", "BAL_TYPE_ID", "DIST_ID", "TOTAL")
    //    dfma.show()
    val freq_list = dfma.select(query_type).distinct().rdd.map(r => r(0).toString).collect()

    val range_s_seq = range_seq.map(_.toString)
    val rpt_not_avail = range_s_seq.toSet.diff(freq_list.toSet).toSeq
    val rpt_avail = range_s_seq.toSet.intersect(freq_list.toSet).toSeq

    rpt_not_avail.foreach(m => logger.info(s"=== Report $query_type $m is not available." ))
    rpt_avail.foreach(filterByFreq(dfma, _))
  }

  def filterByFreq(df: DataFrame, mq: String): Unit = {
    logger.info(s"=== Report for $query_type $mq is being generated ...")
    val m_df = df.filter(query_type + " == " + mq).drop(query_type)
    val rpt_prefix = prefix + "_" + query_type.toUpperCase() +
                    "-" + String.format("%2s", mq).replace(' ', '0')
    writeToCsv(m_df, rpt_prefix)
  }

  def writeToCsv(df: DataFrame, prefix: String) = {
    val fn = data_out + "/" + prefix + "_" +
      LocalDateTime.now().format(DateTimeFormatter.ofPattern("MM.dd.yyyy-HHmmss.SSS"))
    logger.info(s"=== report $fn is generated.")
    df
      .repartition(1)
      .write.format("csv")
      .option("header", "true")
      //      .option("quote", "")
      .mode("overwrite")
      .save(fn)
  }


}

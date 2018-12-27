package com.amway.dms

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.log4j.{Level, Logger}

class RptDriver(data_in: String, data_out: String, mode: String, freq: String, range_seq: Seq[String], prefix: String) {
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
//    val dfm = if (freq == Constant.MM) df1.withColumn(query_type, month(to_date(col("TRX_DT"), "M/d/y")))
//    else df1.withColumn(query_type, quarter(to_date(col("TRX_DT"), "M/d/y")))


    val dfm = if (freq == Constant.MM)
         df1.withColumn(query_type,      // for montly report, get year and month directly from trx_dt column
           concat(
             regexp_extract(col("trx_dt"), "(\\d+)/\\d+/(\\d{4})", 2),
             lit("M"),
             lpad(
               regexp_extract(col("trx_dt"), "(\\d+)/\\d+/(\\d{4})", 1),
               2,"0"
             )
           )
         )  else
      df1.withColumn(query_type,       // for quarterly reports, pull quarter from trx_dt.
        concat(
          regexp_extract(col("TRX_DT"), "(\\d+)/\\d+/(\\d{4})", 2),
          lit("Q"),
          quarter(to_date(col("TRX_DT"), "M/d/y"))
        )
      )
    val dfma = dfm.na.fill(0, Seq(query_type))
    // at this point, we should have column "month" like 2018M09 or column "quarter" like 2018Q3, ready for aggregation.

    val (last_m, last_q) = Utils.mq
    val morq = if (query_type == Constant.MM) last_m else last_q
    val query_filter = mode match {
      case Constant.LAST =>
            query_type + " == '" + morq + "'"
      case Constant.ALL =>
            query_type + " <= '" + morq + "'"
    }

    val dfmb = aggByQueryType(dfma.filter(query_filter).drop(query_type))
    PreSink(dfmb, morq )
    // at this point, prev/all months or prev/all quarters data has been aggregated.


//    val freq_list = dfmb.select(query_type).distinct().rdd.map(r => r(0).toString).collect()

//    val range_s_seq = range_seq.map(_.toString)
//    val rpt_not_avail = range_s_seq.toSet.diff(freq_list.toSet).toSeq
//    val rpt_avail = range_s_seq.toSet.intersect(freq_list.toSet).toSeq
//
//    rpt_not_avail.foreach(m => logger.info(s"=== Report $query_type $m is not available." ))
//    rpt_avail.foreach(filterByFreq(dfma, _))
  }

  def PreSink(df: DataFrame, mq: String): Unit = {
    logger.info(s"=== Report for $query_type $mq is being generated ...")
    val rpt_prefix = prefix + "_" + mode + "_" + query_type.toUpperCase() + "-" + mq
    writeToCsv(df, rpt_prefix)
  }


  def filterByFreq(df: DataFrame, mq: String): Unit = {
    logger.info(s"=== Report for $query_type $mq is being generated ...")
    val m_df = df.filter(query_type + " == " + mq).drop(query_type)
    val rpt_prefix = prefix + "_" + mode + "_" + query_type.toUpperCase() +
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

  def aggByQueryType(df: DataFrame): DataFrame = {
    df
      .groupBy("TRX_ISO_CNTRY_CD", "DIST_ID", "BAL_TYPE_ID")
      .agg(expr("sum(bal_amt) as TOTAL"))
      .withColumn("TOTAL", regexp_replace(format_number(col("TOTAL"), 2), ",", ""))
      .orderBy("TRX_ISO_CNTRY_CD", "BAL_TYPE_ID", "DIST_ID", "TOTAL")
    //    dfma.show()
  }

}

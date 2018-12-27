package com.amway.dms

object Rpt {

  def main(args: Array[String]) {
    val (opt_map, arg_list) = Utils.parse(args)

    val data_in = opt_map('input).toString
    val data_out = opt_map('output).toString
    val mode = opt_map('mode).toString.toLowerCase
    val freq = opt_map('freq).toString.toLowerCase
    val range = if (opt_map.contains('range)) opt_map('range) else None
    val range_seq = Utils.getRange(range, mode, freq)
    val prefix = if (opt_map.contains(Symbol("report-gen-prefix"))) opt_map(Symbol("report-gen-prefix")).toString
                  else Constant.PREFIX
//    range_seq.foreach(println)
    val rpt = new RptDriver(data_in, data_out, mode, freq, range_seq, prefix)
    rpt.init
  }

}
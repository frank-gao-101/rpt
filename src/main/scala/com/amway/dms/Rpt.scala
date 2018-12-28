package com.amway.dms

object Rpt {

  def main(args: Array[String]) {
    val (opt_map, arg_list) = Utils.parse(args)

    val (data_in,
     data_out,
     mode,
     freq,
     range,
     prefix
    ) = Utils.verifyArgs(opt_map)

    //    range_seq.foreach(println)
//    val rpt = new RptDriver(data_in, data_out, mode, freq, range_seq, prefix)
//    rpt.init
  }

}
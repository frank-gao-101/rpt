package com.amway.dms


/*
   parameters:

   args(0):  mode - either cron (0) or ad-hoc (1)
   args(1):  frequency - monthly(0) or quarterly (1)
   args(2):  full path input file
   args(3):  full path output directory

 */

object Rpt {
  def main(args: Array[String]) {

    if (args.length != 4) {
      println("Usage:  <program name> mode frequency Input Output")
      System.exit(1)
    }
    var mode: Int = 0
    var freq: Int = 0
    var data_in: String = ""
    var data_out: String = ""
    try {
      mode = args(0).toInt

      if (mode != 1 && mode != 2) {
        println(mode)
        println("mode is incorrect, please choose 1 or 2")
        System.exit(1)
      }
      freq = args(1).toInt
      data_in = args(2) // Should be some file on your system
      data_out = args(3)
    } catch {
      case _: Throwable => println("bad arguments")
    }

    val rpt = new RptDriver(mode, freq, data_in, data_out)
    rpt.init
  }
}

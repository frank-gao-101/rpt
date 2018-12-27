package com.amway.dms

import java.util.Calendar

object Utils {

  def parse(args: Array[String]): (Map[Symbol, Any], List[Symbol]) = {
    val usage =
      """
    Usage: <program> --input <full_path_file>
            --output <output_dir>
            --mode <last/uptolast>
            --freq <month/quarter>
            [--range <1,2,3-7,8-10,11> ]
            [--report-gen-prefix DMS_ ]
      """
    val map: Map[Symbol, Any] = Map()
    val list: List[Symbol] = List()

    def _parse(map: Map[Symbol, Any], list: List[Symbol], args: List[String]): (Map[Symbol, Any], List[Symbol]) = {
      args match {
        case Nil => (map, list)
        case arg :: value :: tail if (arg.startsWith("--") && !value.startsWith("--")) =>
                      _parse(map ++ Map(Symbol(arg.substring(2)) -> value), list, tail)
    //    case arg :: tail if (arg.startsWith("--")) => _parse(map ++ Map(Symbol(arg.substring(2)) -> true), list, tail)
        case opt :: tail => _parse(map, list :+ Symbol(opt), tail)
      }
    }

    val (opt_map, args_list) = _parse(map, list, args.toList)

//    println(opt_map)
//    println(args_list)
    if (opt_map.size < 4 ||
      !opt_map.contains('input) ||
      !opt_map.contains('output) ||
      !opt_map.contains('mode) ||
      !Seq(Constant.LAST, Constant.ALL).contains(opt_map('mode).toString.toLowerCase) ||
      !opt_map.contains('freq) ||
      !Seq(Constant.MM, Constant.QQ).contains(opt_map('freq).toString.toLowerCase) ||
      (opt_map.contains('range) &&
      ("""^[0-9]([0-9,\-,\,]*[0-9])*$""".r).findFirstIn(opt_map('range).toString) == None) ||
      (opt_map.contains(Symbol("report-gen-prefix")) &&
        opt_map(Symbol("report-gen-prefix")) == None)
    ) {
      println(usage)
      sys.exit(1)
    }
    (opt_map, args_list)
  }

  private def rangex(str: String): Seq[Int] =
    str split ",\\s*" flatMap { (s) =>
      val r = """(-?\d+)(?:-(-?\d+))?""".r
      val r(a, b) = s
      if (b == null) Seq(a.toInt) else a.toInt to b.toInt
    }

  // return a tuple like (2018M09, 2018Q3)
  def mq(): (String, String) = {
    val cal_month = Calendar.getInstance().get(Calendar.MONTH)
    val cal_year = Calendar.getInstance().get(Calendar.YEAR)

    val last_month = if (cal_month == 0) (cal_year - 1).toString + "M12" else (cal_year).toString + "M" + f"$cal_month%02d"
    val curr_month = cal_month + 1
    val curr_q = if (curr_month % 3 == 0) curr_month / 3 else curr_month / 3 + 1
    val last_q = if (curr_q == 1) (cal_year - 1).toString + "Q4" else cal_year.toString + "Q" + (curr_q - 1).toString

    (last_month, last_q)
  }

  def getRange(range: Any, mode: String, freq: String): Seq[String] = {
    val (last_month, last_q) = mq

     Seq[String]()

//    val range_seq = range match {
//      case None =>
//        mode match {
//          case Constant.LAST => { // previous month or quarter
//            freq match {
//              case Constant.MM => Seq(last_month)
//              case Constant.QQ => Seq(last_q)
//            }
//          }
//          case Constant.ALL => { // up to previous month or quarter
//            freq match {
//              case Constant.MM => 1 to 12
//              case Constant.QQ => 1 to 4
//            }
//          }
//        }
//      case _ => {
//        val r = rangex(range.toString)
//        freq match {
//          case Constant.MM => r.toSet.intersect((1 to 12).toSet).toSeq
//          case Constant.QQ => r.toSet.intersect((1 to 4).toSet).toSeq
//        }
//      }
//    }
//    range_seq.toString
  }
}

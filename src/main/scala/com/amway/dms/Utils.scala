package com.amway.dms

import java.util.Calendar

object Utils {

  def parse(args: Array[String]): (Map[Symbol, Any], List[Symbol]) = {
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

    (opt_map, args_list)
  }

  def verifyArgs(opt_map: Map[Symbol, Any]) : (String,  //input
    String, //output
    Option[Any], // mode
    Option[Any], // freq
    Seq[String], // range
    Option[Any] // prefix
    ) = {
    val usage =
      """
        | Usage: <program> --input <full_path_file>
        |     --output <output_dir>
        |     --mode <last/upto/all>
        |     --freq <month/quarter>
        |     [--range <2017Q1, 2017M01-2018M12> ]
        |     [--report-gen-prefix DMS ]
      """

    if (!opt_map.contains('input) || !opt_map.contains('output)) {
      println(Constant.ERR_INPUT_OUTPUT_MISSING);
      println(usage)
      sys.exit(1)
    }

    if (!opt_map.contains('range) && !opt_map.contains('mode) ||
      !Seq(Constant.LAST, Constant.ALL, Constant.UPTO).contains(opt_map('mode).toString.toLowerCase)) {
      println(Constant.ERR_INVALID_MODE);
      println(usage)
      sys.exit(1)
    }

    if (!opt_map.contains('range) && !opt_map.contains('freq) ||
      !Seq(Constant.MM, Constant.QQ).contains(opt_map('freq).toString.toLowerCase)) {
      println(Constant.ERR_INVALID_FREQ);
      println(usage)
      sys.exit(1)
    }

    (opt_map('input).toString,
      opt_map('output).toString,
      opt_map.get('mode),
      opt_map.get('freq),
      rangexpr(opt_map.get('range)),
      opt_map.get(Symbol("report-gen-prefix")).orElse(Some(Constant.PREFIX))

    )
  }

  /*
     a:  the entire left part of the range
     b:  year of the left
     c:  delimiter
     d:  month of the left
     e:  the entire right part of the range
     f:  year of the right
     g:  month of the right
   */
  def rangexpr(str: Option[Any]): Seq[String] =
    if (str.isEmpty) Seq() else {
       str.get.toString split ",\\s*" flatMap { (s) =>
        val r = """((\d{4})([M|Q])(\d{1,2}))(?:-((\d{4})\3(\d{1,2})))?""".r
        s match {
          case r(a, b, c, d, e, f, g) if e == null =>
            if (validRange(c, d)) Seq(a) else Seq()
          case r(a, b, c, d, e, f, g) =>
            if (validRange(c, d) && validRange(c, g)) expandRange(b, c, d, f, g) else Seq()
          case _ => Seq()
        }
      }
    }

  def expandRange(y1: String, delim: String, m1: String, y2: String, m2: String): Seq[String] = {
    val carry = delim match {
      case Constant.M => 12
      case Constant.Q => 4
    }

    val diff = (y2.toInt - y1.toInt) * carry + (m2.toInt - m1.toInt) + 1

    if (diff < 1) Seq() else genRangeList(y1.toInt, m1.toInt, carry, diff, delim)
  }

  def genRangeList(y: Int, m: Int, carry: Int, diff: Int, delim: String): Seq[String] = {
    val init_seq = Seq[(Int, Int)]()

    def gen(y: Int, m: Int, n: Int, s: Seq[(Int, Int)]): Seq[(Int, Int)] = {
      n match {
        case 0 => s :+ (y, m)
        case _ if m > carry => gen(y + 1, m - carry, n - 1, s)
        case _ => gen(y, m + 1, n - 1, s :+ (y, m))
      }
    }

    val seq_tuple = gen(y, m, diff, init_seq)
    seq_tuple.toList.map(m => (m._1).toString + delim + (if (delim == Constant.M) f"${m._2}%02d" else m._2))
  }

  def validRange(unit: String, num_str: String): Boolean = {
    val num = num_str.toInt
    unit match {
      case Constant.M if num > 0 && num < 13 => true
      case Constant.Q if num > 0 && num < 5 => true
      case _ => false
    }
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

}

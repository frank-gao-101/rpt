package com.amway.dms

object Rpt {
  def main(args: Array[String]) {
    val (opt_map, arg_list) = parse(args)
    val mode = opt_map('mode)
    val freq = opt_map('freq)
    val data_in = opt_map('input)
    val data_out = opt_map('output)
    val range = if (opt_map.contains('range)) opt_map('range) else None

    //    val rpt = new RptDriver(mode, freq, data_in, data_out)
    //    rpt.init
  }

  def parse(args: Array[String]): (Map[Symbol, Any], List[Symbol]) = {
    val usage =
      """
    Usage: <program> --input <full_path_file>
            --output <output_dir>
            --mode <last/uptolast>
            --freq <month/quarter> [--range <1,2,3-7,8-10,11> ]
  """
    val map: Map[Symbol, Any] = Map()
    val list: List[Symbol] = List()

    def _parse(map: Map[Symbol, Any], list: List[Symbol], args: List[String]): (Map[Symbol, Any], List[Symbol]) = {
      args match {
        case Nil => (map, list)
        case arg :: value :: tail if (arg.startsWith("--") && !value.startsWith("--")) => _parse(map ++ Map(Symbol(arg.substring(2)) -> value), list, tail)
        case arg :: tail if (arg.startsWith("--")) => _parse(map ++ Map(Symbol(arg.substring(2)) -> true), list, tail)
        case opt :: tail => _parse(map, list :+ Symbol(opt), tail)
      }
    }

    val (opt_map, args_list) = _parse(map, list, args.toList)

    if (opt_map.size < 4 ||
         !opt_map.contains('mode) ||
         !Seq("last", "uptolast").contains(opt_map('mode)) ||
         !opt_map.contains('freq) ||
         !Seq("month", "quarter").contains(opt_map('freq)) ||
         !opt_map.contains('range) ||
         ("""^[0-9]([0-9,\-,\,]*[0-9])*$""".r).findFirstIn(opt_map('range).toString) == None
        ) {
      println(usage)
      sys.exit(1)
    }

    (opt_map, args_list)

  }


}
package cc.xmccc.sparkdemo

import org.apache.hadoop.hbase.util.Bytes

object Utils {
  def repr_string(arr: String): String = {
    def _interpret(c: Char): String = BigInt(c) match {
      case x if x >= 32 && x <= 126 =>
        String.valueOf(Character.toChars(x.toInt))
      case x =>
        "\\x%02x" format x
    }
    arr.map(_interpret).mkString("")
  }
}

import Utils._

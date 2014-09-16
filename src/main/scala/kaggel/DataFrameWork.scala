package kaggel

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import df.DataFrame._
import utils.Utils
import df.DataFrame
import df.VarType

object DataFrameSummary {

  def main(args: Array[String]) = {
    val sc = new SparkContext(new SparkConf().setAppName("KaggelApp"))
    val df = DataFrame.fromCSV(sc.textFile(args(0)))(KaggelIO.typeGuesser) 
    val summary = df.summary()
    summary.print()   
    Utils.writeObject(args(1))(summary)
  }

}

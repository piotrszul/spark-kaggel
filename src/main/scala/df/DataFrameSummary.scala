package df

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.reflect.ClassTag
import DataFrame._
import utils.Utils

object DataFrameSummary {

  def main(args: Array[String]) = {

    val sc = new SparkContext(new SparkConf().setAppName("KaggelApp"))
    val df = DataFrame.fromCSV(sc.textFile(args(0))) {
      case (i, n) =>
        if (n.startsWith("C") || i == 0) VarType.Categorical else VarType.Numerical
    };
    val summary = df.summary()
    summary.print()   
    Utils.writeObject(args(1))(summary)
  }

}

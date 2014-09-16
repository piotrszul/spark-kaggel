package ml

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object Eval {
  val epsilon = 1e-5
  
  def logLogs(pred: RDD[(Double, Double)]): Double = pred.map({
    case (p, a) =>
      math.min(math.max(p, epsilon), 1 - epsilon)
      -math.log(p) * a - math.log(1 - p) * (1 - a)
  }).mean()

}
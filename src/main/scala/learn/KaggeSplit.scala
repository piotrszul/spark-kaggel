package learn

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.io.ObjectOutputStream
import java.io.FileOutputStream
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.classification.SVMWithSGD
import scala.util.Sorting
import java.util.Arrays
import org.apache.spark.rdd.RDD
import java.io.InputStream
import java.io.Closeable
import java.io.ObjectInputStream
import java.io.FileInputStream

object KaggelSplit {
  
  def use[F <: Closeable,T](file:F)(handler:(F=>T)):T = {
    try {
      return handler(file)
    } finally {
      file.close()
    }
  }
  
  def add(t1:(Long,Long), t2:(Long,Long)):(Long,Long) = (t1._1 + t2._1, t1._2 + t2._2)
  def addd(t1:(Long,Double), t2:(Long,Double)):(Long,Double) = (t1._1 + t2._1, t1._2 + t2._2)
  
  def main(args: Array[String]) {

    
    val sc = new SparkContext(new SparkConf().setAppName("KaggelApp"))
    val in = sc.textFile(args(0))
    val data = in.filter(t => !t.startsWith("Id"))
    val split = data.randomSplit(Array(0.85, 0.15), 100)
    split(0).saveAsTextFile(args(0)+ "_1")
    split(1).saveAsTextFile(args(0)+ "_2")
  }
} 

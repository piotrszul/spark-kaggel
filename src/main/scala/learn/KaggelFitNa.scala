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

object FitNa {
  
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
    val input  = in.map(_.split(","))
    val header = input.first().toList
    val data = input.filter(t => !t(0).startsWith("Id"))    
    println(data.count())
    
    val df = data.map{t => 
      val padded = t.iterator.padTo(header.length, "")
      val vals = header.iterator.zip(padded)
      vals.map{ case (col, str) => 
        if (str.isEmpty()) null else if (col.startsWith("C")) java.lang.Long.parseLong(str, 16) else str.toLong 
      }.toList
    }
    
   val model:NaModel = use(new ObjectInputStream(new FileInputStream("na.model"))) { f =>
     f.readObject().asInstanceOf[NaModel]
   }

   val result = model.fit(df)
   result.map(_.mkString(",")).saveAsTextFile(args(1))
   
  }
} 

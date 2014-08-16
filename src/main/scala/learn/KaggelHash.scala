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

object KaggelHashLog {
  
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
        if (str.isEmpty()) null else if (col.startsWith("C")) java.lang.Long.parseLong(str, 16) else str.toInt 
      }.toList
    }
    
    val split = df.randomSplit(Array(0.8,0.2), 10)
    
    val b = 26
    val len = 30 + (2<<b)
    println(2<<b, len)
    
    val train = split(0).map{t => 
      new LabeledPoint(t(1).asInstanceOf[Long].toDouble,
          Vectors.sparse(len, 
              t.drop(2).take(len).zipWithIndex.filter(_._1 != null)
              .map{ case (v:Long,i) => 
                //println(i, v, (30 + (v.toInt & ((2<<b)-1))))
                if (i< 13) (i,v.toDouble) else ((30 + ((v.toInt+i)  & ((2<<b)-1))), 1.0)
             }
          )           
      )
    }.cache()
 
    
    val test = split(1).map{t => 
      new LabeledPoint(t(1).asInstanceOf[Long].toDouble,
          Vectors.sparse(len, 
              t.drop(2).take(len).zipWithIndex.filter(_._1 != null)
              .map{ case (v:Long,i) => 
                if (i< 13) (i,v.toDouble) else ((30 + ((v.toInt+i)  & ((2<<b)-1))), 1.0)
             }
          )
      )
    }.cache()
    
    println(train.count)   
    println(test.count)   
    val model = LogisticRegressionWithSGD.train(train, 100)
    println(model.weights)
    
    val result = model.predict(test.map(_.features)).zip(test.map(_.label))
    
    val metrics = new BinaryClassificationMetrics(result)
    println(metrics.recallByThreshold.collect().toList)
    println(metrics.precisionByThreshold.collect().toList)
  }
} 

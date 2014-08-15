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

class NaModel(numericalMap:Map[Int,Double],categoricalMap:Map[Int,Any]) extends Serializable {
  
  def fit(rdd:RDD[List[Any]]):RDD[List[Any]] = {
   
    rdd.map(l => l.zipWithIndex.map{case (v,i) =>
      	if (v !=null) v else if (numericalMap.get(i).isDefined) numericalMap(i) else categoricalMap(i)
      }
    )
  }
  
}


object KaggelNoNa {
  
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
    
    
    // fit average fro numerical variables
    
    val sf = df.flatMap(t =>
      	t.zipWithIndex.drop(2).take(13).filter(_._1 != null).map{case (v,i) => (i, (1L, v.asInstanceOf[Long].toDouble))}
    ).reduceByKey(addd).map{ case (i, (c,v)) => (i,v.toDouble/c.toDouble )}  

        
    val numericalAverages = sf.collectAsMap().toMap
    
    // fit most frequent values
    
    val cf = df.flatMap(t =>
      	t.zipWithIndex.drop(15).map(t =>(t.swap, 1L))
    ).reduceByKey(_ + _).map{case ((i,v), c) => (i, (c,v))}
    .reduceByKey((t1,t2) => if (t1._1 > t2._1) t1 else t1 ).map{case (i,(c,v)) => (i,v) }
      	
   val categoricalModes = cf.collectAsMap().toMap
   numericalAverages.foreach(println)
   categoricalModes.foreach(println)
 
   val model = new NaModel(numericalAverages, categoricalModes)
   
   use(new ObjectOutputStream(new FileOutputStream("na.model"))) { f =>
     f.writeObject(model)
   }
    
  }
} 

package learn

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.io.ObjectOutputStream
import java.io.FileOutputStream
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.classification.SVMWithSGD
import scala.util.Sorting
import java.util.Arrays
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.Ex

object KaggelLogLearn {
  
  def add(t1:(Long,Long), t2:(Long,Long)):(Long,Long) = (t1._1 + t2._1, t1._2 + t2._2)
  def addd(t1:(Long,Double), t2:(Long,Double)):(Long,Double) = (t1._1 + t2._1, t1._2 + t2._2)
  
  def main(args: Array[String]) {

    
    val sc = new SparkContext(new SparkConf().setAppName("KaggelApp"))

    /*
     *     [,1] [,2]
[1,]  1.0  0.0
[2,]  0.5  0.5
[3,]  0.0  1.0
[4,]  2.0  0.0
[5,]  1.0  1.0
[6,]  0.0  2.0
     */
    
    val train  = sc.parallelize(List(
    	new LabeledPoint(0.0, Vectors.dense(Array(1.0, 0.0))),
    	new LabeledPoint(0.0, Vectors.dense(Array(0.5, 0.5))),
    	new LabeledPoint(0.0, Vectors.dense(Array(0.0, 1.0))),
    	new LabeledPoint(1.0, Vectors.dense(Array(2.0, 0.0))),
    	new LabeledPoint(1.0, Vectors.dense(Array(1.0, 1.0))),    	
    	new LabeledPoint(1.0, Vectors.dense(Array(0.0, 2.0)))
    )) 

    val test  = sc.parallelize(List(
    	new LabeledPoint(1.0, Vectors.dense(Array(0.0,10.0))),
    	new LabeledPoint(0.0, Vectors.dense(Array(1.0,-5.0)))
    )) 
    
    
    println(train.count)   
    println(test.count)   
    
    val cls = new LogisticRegressionWithSGD()
    cls.setIntercept(true)
    val model = cls.run(train)
    //val model = LogisticRegressionWithSGD.train(train,100)
    println("Weights")
    println(model.weights)
    println(model.intercept)
    
    val result = Ex.predictProb(model,test.map(_.features)).zip(test.map(_.label))
    
    println("Result")
    result.take(1000).foreach(println)
    
    val metrics = new BinaryClassificationMetrics(result)
    println(metrics.recallByThreshold.collect().toList)
    println(metrics.precisionByThreshold.collect().toList)
  }
} 

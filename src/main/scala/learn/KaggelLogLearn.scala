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
    
    
    val train  = sc.textFile("data/wisc_train_all.csv")
    	.map(_.split(",").map(_.toDouble).toList).map(t=>new LabeledPoint(t(0),
    			Vectors.dense(t.drop(1).toArray)
    	)).cache()

    val test  = sc.textFile("data/wisc_test_all.csv")
    	.map(_.split(",").map(_.toDouble).toList).map(t=>new LabeledPoint(t(0),
    			Vectors.dense(t.drop(1).toArray)
    ))
    
    
    println(train.count)   
    println(test.count)   
    
    val cls = new LogisticRegressionWithSGD()
    cls.setIntercept(true)
    cls.optimizer.setStepSize(100.0)
    cls.optimizer.setNumIterations(2000)
    val model = cls.run(train)
    //val model = LogisticRegressionWithSGD.train(train,100)
    println("Weights")
    println(model.weights)
    println(model.intercept)
    
    
    val result = Ex.predictProb(model,test.map(_.features)).zip(test.map(_.label))
    
    //println("Result")
    //result.take(100).foreach(println)
    
    val epsilon = 1e-5
    val logLoss = result.map({
      case (p,a) =>
        val mp = math.min(math.max(p,epsilon), 1-epsilon)
        -math.log(mp)*a-math.log(1-mp)*(1-a)}).mean()
    
    println("Log lodss: " + logLoss) 
        
    /*
    val metrics = new BinaryClassificationMetrics(result)
    println(metrics.recallByThreshold.collect().toList)
    println(metrics.precisionByThreshold.collect().toList)
  
  * 
  */
    }
} 

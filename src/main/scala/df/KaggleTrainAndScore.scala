package df

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.Ex

object KaggleTrainAndScore {
	def addd(t1:(Long,Double), t2:(Long,Double)):(Long,Double) = (t1._1 + t2._1, t1._2 + t2._2)

	val epsilon = 1e-15
  
    def logit(predict:Double,act:Double):Double = {
    		val pred = math.min(math.max(predict, epsilon), 1-epsilon)
    		return -act*math.log(pred) -(1.0-act)*math.log(1.0-pred)
    }
  
    def main(args: Array[String]) = {

    val sc = new SparkContext(new SparkConf().setAppName("KaggelApp"))
    val df = DataFrame.fromCSV(sc.textFile(args(0))) {
      case (i, n) =>
        if (n.startsWith("C") || i == 0) VarType.Categorical else VarType.Numerical
    };


    val summary = df.summary().summary.drop(2).take(13).map{_.asInstanceOf[NumericalSummary]}
    
    val fullSet = df.rdd.map { r =>
      new LabeledPoint(r(1).asInstanceOf[Double],Vectors.dense(
    		  r.drop(2).take(2).toList.asInstanceOf[List[Double]].map(v => if (v.isNaN()) 0.0 else v)
    		  .zipWithIndex.map{case (v,i) => (v-summary(i).min)/(summary(i).max - summary(i).min) }
    		  .toArray)
          )
    }

    val split = fullSet.randomSplit(Array(0.8,0.2), 1);
    val trainSet = split(0).cache()
    val testSet = split(1).cache()
    /*
    trainSet
    	.map(v => v.label.toString+"," + v.features.toArray.toList.mkString(","))
    	.saveAsTextFile("target/train.csv")
    */
    println("Train set: " + trainSet.count())
    println("Test set: " + testSet.count())
    
    val cls = new LogisticRegressionWithSGD()
    cls.setIntercept(true)
    cls.setValidateData(true)
    cls.optimizer.setNumIterations(500)
    //cls.optimizer.setRegParam(10)
    val model = cls.run(trainSet)

    println(model)
    println(model.intercept)
    println(model.weights)
    
    val result = Ex.predictProb(model,trainSet.map(_.features)).zip(trainSet.map(_.label))
    
    println("Result")
    result.take(100).foreach(println)

    val scoreTouple = result.mapPartitions{i => 
      var sum:Double = 0.0
      var count:Long = 0L
      i.foreach {d => sum+= logit(d._1,d._2);count+=1}
      Some((count,sum)).iterator
    }.reduce(addd)
    val score = scoreTouple._2/scoreTouple._1
    println("Score: " + score)
    
  }

}
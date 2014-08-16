package org.apache.spark.mllib

import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector

object Ex {
  def predictProb(m:LogisticRegressionModel,rdd:RDD[Vector]):RDD[Double] = 
	  rdd.map{v =>
	   	val margin = m.weights.toBreeze.dot(v.toBreeze) + m.intercept
	   	println(margin)
	   	1.0 / (1.0 + math.exp(-margin))
  	}
}
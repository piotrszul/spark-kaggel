package kaggel

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import df.DataFrame._
import utils.Utils
import df.DataFrame
import df.VarType
import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import it.unimi.dsi.fastutil.longs.LongSet
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2LongMap
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap
import df.ColumnDef
import df.CategoricalVarType
import df.NumericalSummary
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.Ex
import ml.Eval
import org.apache.commons.math3.analysis._
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType
import org.apache.commons.math3.optim.MaxEval
import org.apache.commons.math3.optim.univariate.UnivariateObjectiveFunction
import org.apache.commons.math3.optim.univariate.SearchInterval
import org.apache.spark.mllib.optimization.SquaredL2Updater
import scala.collection.JavaConverters._
import scala.math.Numeric
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.SparseVector
import ml.GlobalIndexer
import ml.Pipe
import ml.FillNa
import ml.LogScaler
import ml.MinMaxScaler
import ml.FilterLevles



object DataFrameWork {

  
   
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("KaggelApp"))
    val df = DataFrame.fromCSV(sc.textFile(args(0)))(KaggelIO.typeGuesser)

    val rdd = df.rdd;
    implicit val cols = df.cols
/*
    val result: Array[LongOpenHashSet] = rdd.aggregate(Array.fill(cols.size)(new LongOpenHashSet()))(
      { (s: Array[LongOpenHashSet], v: List[Any]) =>
        v.zipWithIndex.filter(k => k._2> 0 && cols(k._2).isCategorical)
          .foreach({ case (v, i) => if (!cols(i).varType.isNaN(v)) s(i).add(v.asInstanceOf[Long]) }); s
      },
      { (s1: Array[LongOpenHashSet], s2: Array[LongOpenHashSet]) => s1.zip(s2).foreach(k => k._1.addAll(k._2)); s1 })

    //result.map(_.size()).foreach(println)
    val indexMapping = result.map(reindex)

    val indexBrd = sc.broadcast(indexMapping)
    val mapped = rdd.map(categorical{ case (v,i) => indexBrd.value(i).get(v)})

    //mapped.map(_.mkString(",")).saveAsTextFile(args(1))

    println(mapped.count())

*/
    val mapped = rdd
    val mappedDf = new DataFrame(cols,mapped)
    
    val indexer = new GlobalIndexer()
    val preprocess = new Pipe(List(
        new FillNa(0), new LogScaler(), new MinMaxScaler(), new FilterLevles(15), indexer
        ))
    
    preprocess.fit(mappedDf)
    val yy = preprocess.transform(mappedDf)
  
    yy.rdd.take(10).foreach(println)
    //exit(0)
    val size = indexer.size
    println("Size: " + size)
    
    val hashSize = 1<<12
    val hashMask = hashSize - 1 

/*    
    val dataPre = yy.rdd.map{ l=> 
      new LabeledPoint(l(1).asInstanceOf[Double], 
          Vectors.sparse(hashSize + 13, l.drop(2).zipWithIndex.filter({ case (v,i) => (i< 13 || v.asInstanceOf[Long] >0)  }
            ).map({ case (v,i)=>
            if (i< 13) (i,v.asInstanceOf[Double]) else (13 + (v.hashCode()&hashMask).toInt,if ((v.hashCode()&1) == 1) 1.0 else -1.0)
          }).groupBy(_._1).map({case (i,l) => (i,l.map(_._2).sum)}).filter(_._2 != 0).toSeq)
          ) 
      }
      
      */
    val dataPre = yy.rdd.map{ l=> 
      new LabeledPoint(l(1).asInstanceOf[Double], 
          Vectors.sparse(size  + 13, l.drop(2).zipWithIndex.filter({ case (v,i) => (i< 13 || v.asInstanceOf[Long] >=0)  }
            ).map({ case (v,i)=>
            if (i< 13) (i,v.asInstanceOf[Double]) else (13 + v.asInstanceOf[Long].toInt,if ((v.hashCode()&1) == 1) 1.0 else -1.0)
          }).toSeq)
          ) 
      }
 
    val data = if (args.length > 1) dataPre.coalesce(args(1).toInt) else dataPre
    //val data = yy.rdd
    val split = data.randomSplit(Array(0.75, 0.25), 3334)
    val train = split(0)
    val test = split(1)
    
    /*
    train.cache()
    println(train.count())
    test.cache()
    println(test.count())
    */
    
    val trainTxt = train.map{l =>
      val sv:SparseVector = l.features.asInstanceOf[SparseVector]
      (if (l.label >0) 1 else -1).toString + " | " + sv.indices.toIterator
      	.zipWithIndex.map(p => (p._1,sv.values(p._2))).map(t => t._1.toString + ":"+ t._2.toString).mkString(" ")
    }  
    val testTxt = test.map{l =>
      val sv:SparseVector = l.features.asInstanceOf[SparseVector]
      (if (l.label >0) 1 else -1).toString + " | " + sv.indices.toIterator
      	.zipWithIndex.map(p => (p._1,sv.values(p._2))).map(t => t._1.toString + ":"+ t._2.toString).mkString(" ")
    }  
    
    trainTxt.saveAsTextFile("target/trainTxt")
    testTxt.saveAsTextFile("target/testTxt")
    
    //new DataFrame(yy.cols, train).toVW("Label","Id").saveAsTextFile("target/train_01_vw")
    //new DataFrame(yy.cols, test).toVW("Label","Id").saveAsTextFile("target/test_01_vw")
    
    exit(0)
 /*   
    train.take(10).foreach(println)
    
    
    val cls = new LogisticRegressionWithSGD()
    cls.setIntercept(true)
    cls.optimizer.setNumIterations(100)
    cls.optimizer.setUpdater(new SquaredL2Updater())
    cls.optimizer.setRegParam(6.38021917904243E-4)
    cls.optimizer.setStepSize(20.976103)
    
    def goal(a:Double):Double = {
      
        println("Optimizigin for: " + a)
	    cls.optimizer.setStepSize(a) 
        //cls.optimizer.setRegParam(a)
	    
	    val means = for(i <-1 to 3 ) yield {
	      
	    val split = train.randomSplit(Array(0.75,0.25), i)  
	    val model = cls.run(split(0))
	    val pred = Ex.predictProb(model,split(1).map(_.features)).zip(split(1).map(_.label))    
	    Eval.logLogs(pred)
	      
	    }
	   val logLoss  = means.toList.reduce( _ + _)/means.size 
	   println("Mean log loss:" + logLoss + " for: " + a)
	   logLoss
    }
    val optimize = false
    if (optimize) {
	    val optmizer = new org.apache.commons.math3.optim.univariate.BrentOptimizer(0.001, 0.0001)
	    val result = optmizer.optimize(new MaxEval(20), 
	    		new UnivariateObjectiveFunction(new UnivariateFunctionAdapter(goal)), 
	    		GoalType.MINIMIZE, new SearchInterval(1,50))
	    
	    //val opa = optimize(1,200,0.0001)(goal)
	    //val opa = goal(64.0)
	    println("OptLearning Rate: " + result.getPoint() + " with value" + result.getValue())    
	    //println("Log Loss: " + opa)  
	    cls.optimizer.setStepSize(result.getPoint())
    }
    cls.optimizer.setNumIterations(200)
    val finalModel = cls.run(train)
    val finalPred = Ex.predictProb(finalModel,test.map(_.features)).zip(test.map(_.label))    
    val finalLoss = Eval.logLogs(finalPred)
    println("Final loss:" + finalLoss)
    
    exit(0)
    * 
    */
  }

}

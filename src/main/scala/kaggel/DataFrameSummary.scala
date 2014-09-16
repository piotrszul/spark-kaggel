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
import Helpers._
import df.NumericalSummary
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.Ex
import ml.Eval


object Helpers {
  def categorical[T](f: (Long, Int) => Long)(implicit cols: Seq[ColumnDef]): List[Any] => List[Any] = {
    return { l =>
      l
        .zipWithIndex
        .map {
          case (v, i) =>
            if (i> 0 && cols(i).isCategorical) f(v.asInstanceOf[Long], i) else v
        }
    }
  }
   def numerical[T](f: (Double, Int) => Double)(implicit cols: Seq[ColumnDef]): List[Any] => List[Any] = {
    return { l =>
      l
        .zipWithIndex
        .map {
          case (v, i) =>
            if (!cols(i).isCategorical) f(v.asInstanceOf[Double], i) else v
        }
    }
  }

  def numericalWithType[T](f: (Double, ColumnDef) => Double)(implicit cols: Seq[ColumnDef]): List[Any] => List[Any] = {
    return { l =>
      l
        .zip(cols)
        .map {
          case (v, c) =>
            if (!c.isCategorical) f(v.asInstanceOf[Double], c) else v
        }
    }
  }

}

trait Processor  {
  def fit(df: DataFrame)
  def transform(df: DataFrame):DataFrame
}

object XXX {
  def fillNa(cols:Seq[ColumnDef], rdd:RDD[List[Any]], naval:Double): DataFrame =  {
	  implicit val columns = cols
	  new DataFrame(cols, rdd.map(numericalWithType { case (v,c) => if (c.varType.isNaN(v)) naval else v }))
  }
  def minMaxScaler(cols:Seq[ColumnDef], rdd:RDD[List[Any]],summary:Map[Int, NumericalSummary]):DataFrame = {
	  implicit val columns = cols
	  new DataFrame( cols, rdd.map(numerical { case (v,i) => val s = summary(i); (v-s.min)/(s.max-s.min) }))    
  }
}

class FillNa(naval: Double) extends Processor {
  def fit(f: DataFrame) {}
  def transform(df: DataFrame):DataFrame = XXX.fillNa(df.cols, df.rdd, naval)
}

class MinMaxScaler extends Processor {
 
  var  summary:Map[Int, NumericalSummary] = null
  
  def fit(df: DataFrame) = {
    summary = df.mins()
  }
  def transform(df: DataFrame):DataFrame = XXX.minMaxScaler(df.cols, df.rdd, summary)
}

class Pipe(elements:Seq[Processor]) extends Processor {
  def fit(df: DataFrame) = elements.foreach(_.fit(df)) 
  def transform(df: DataFrame):DataFrame = {
    var result = df
    elements.foreach(e => result = e.transform(result))
    result
  }
  
}




object DataFrameWork {

  def reindex(s: LongSet): Long2LongMap = {
    var index = 0L;
    val result = new Long2LongOpenHashMap()
    result.defaultReturnValue(-1L)
    val it = s.iterator()
    while (it.hasNext()) {
      result.add(it.next(), index)
      index += 1
    }
    return result
  }

  def optimize(l1:Double,p1:Double, l2:Double, p2:Double, tresh:Double)(goal:Double=>Double):Double = {
    if (l2-l1 > tresh) {	    
	    val lm = (l1+l2)/2
	    val vm = goal(lm)
	    if (p1< p2) {
	      optimize(l1,p1,lm,vm, tresh)(goal)
	    } else {
	      optimize(lm,vm, l2,p2, tresh)(goal)      
	    }
    } else {
      if (p1 < p2) l1 else l2
    }
  } 
  
  def optimize(l1:Double, l2:Double, thresh:Double)(goal: Double => Double):Double = {
    val p1 = goal(l1)
    val p2 = goal(l2)
    optimize(l1,p1,l2,p2,thresh)(goal)
  }

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
    val preprocess = new Pipe(List(
        new FillNa(0),new MinMaxScaler()
        ))
    
    preprocess.fit(mappedDf)
    val yy = preprocess.transform(mappedDf)
    
    
    
    
    val train = yy.rdd.map{ l=> new LabeledPoint(l(1).asInstanceOf[Double], Vectors.dense(l.drop(2).take(10).map(_.asInstanceOf[Double]).toArray)) }
    
    train.cache()
    println(train.count())
    
    
 
    
    val cls = new LogisticRegressionWithSGD()
    cls.setIntercept(true)
    cls.optimizer.setNumIterations(100)
    
    def goal(a:Double):Double = {
	    cls.optimizer.setStepSize(a)    
	    val model = cls.run(train)
	    val pred = Ex.predictProb(model,train.map(_.features)).zip(train.map(_.label))    
	    val logLoss = Eval.logLogs(pred)
	    println(logLoss)
	    return logLoss
    }
    
    val opa = optimize(1,200,0.01)(goal)
    println("OptLearning Rate: " + opa)
  }

}

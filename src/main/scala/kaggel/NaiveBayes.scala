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
import scala.collection.JavaConverters._
import scala.math.Numeric




object NaiveBayes {

  def predict(model:Array[Long2LongOpenHashMap], ps:Long, pp:Long,  sample:List[Any]):Double  = {
    
    val pairs = model.zip(sample).filter(_._1.size() > 0).map{ case (m,s) =>
      val value:Long = if (m.containsKey(s)) m.get(s) else m.get(-1L)
      val fs = value >> 32
      val fp = value&0xffffffffL
      (if (fs > 0) fs else 1, if (fp > 0) fp else 1)
    }
    
    //println(pairs.toList)
    
    val cs = pairs.toList.map(_._1.toDouble/ps).reduce( _*_) * ps.toDouble/(ps+pp)
    val cp = pairs.toList.map(_._2.toDouble/pp).reduce( _*_) * pp.toDouble/(ps+pp)
 
    cs/(cs+cp)
  }
  
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

  def optimize(l1:Double,p1:Double, l2:Double, p2:Double, tresh:Double)(goal:Double=>Double):(Double,Double) = {
    if (math.abs(p1-p2) > tresh) {	    
	    val lm = (l1+l2)/2
	    val vm = goal(lm)
	    if (p1< p2) {
	      optimize(l1,p1,lm,vm, tresh)(goal)
	    } else {
	      optimize(lm,vm, l2,p2, tresh)(goal)      
	    }
    } else {
      if (p1 < p2) (l1,p1) else (l2,p2)
    }
  } 
  
  def optimize(l1:Double, l2:Double, thresh:Double)(goal: Double => Double):(Double,Double) = {
    val p1 = goal(l1)
    val p2 = goal(l2)
    optimize(l1,p1,l2,p2,thresh)(goal)
  }

  
  def combine(s1: Array[Long2LongOpenHashMap], s2: Array[Long2LongOpenHashMap]):Array[Long2LongOpenHashMap] ={
    s1.zip(s2).foreach{ case (m1,m2) =>
      val it = m2.entrySet().iterator().asScala.foreach{ next =>
        val key:Long = next.getKey()
        val value:Long = next.getValue()
        m1.add(key,value)
      }
    }
    s1
  }
  
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("KaggelApp"))
    val df = DataFrame.fromCSV(sc.textFile(args(0)))(KaggelIO.typeGuesser)

    val rdd = df.rdd;
    implicit val cols = df.cols

    val result: Array[Long2LongOpenHashMap] = rdd.aggregate(Array.fill(cols.size)(new Long2LongOpenHashMap()))(
      { (s: Array[Long2LongOpenHashMap], v: List[Any]) =>
      	val label = v(1).asInstanceOf[Double].toInt
        v.zipWithIndex.filter(k => k._2 > 0 && cols(k._2).isCategorical)
          .foreach({ case (v, i) => 
            val vl = v.asInstanceOf[Long]
            s(i).add(vl, 1L<<(32*label))  });
      	  s
      }, combine)

    
    println(result(22))
    result.filter(m=>m.size() >0).map({ m =>      
        (m.values().iterator().asScala.map(_&0xffffffffL).sum,
        m.values().iterator().asScala.map(_>>32).sum)
    }).foreach(println)

 
    result.filter(m=>m.size() >0).map({ m => 
 
    	val it = m.entrySet().iterator()
    	var marginals = 0L
    	while(it.hasNext()) {
    	  val e = it.next()
    	  val total = e.getValue().toLong&0xffffffffL + e.getValue().toLong>>32
    	  if (total < 1000) {
    	    marginals += total
    	    it.remove()
    	  }
    	}
    	m.add(-1, marginals)
    })

    println(result(22))
    
    // predict
    
 
    rdd.map(l=>predict(result,10863, 31254,l)).filter(_ > 0.01).collect().foreach(println)
    
    /*
    val mapped = rdd
    val mappedDf = new DataFrame(cols,mapped)
    val preprocess = new Pipe(List(
        new FillNa(0), new LogScaler(), new MinMaxScaler()
        ))
    
    preprocess.fit(mappedDf)
    val yy = preprocess.transform(mappedDf)
    
    val dataPre = yy.rdd.map{ l=> new LabeledPoint(l(1).asInstanceOf[Double], Vectors.dense(l.drop(2).take(13).map(_.asInstanceOf[Double]).toArray)) }
   
    val data = if (args.length > 1) dataPre.coalesce(args(1).toInt) else dataPre
    val split = data.randomSplit(Array(0.75, 0.25), 3334)
    val train = split(0)
    val test = split(1)
    train.cache()
    println(train.count())
    test.cache()
    println(test.count())
    
    train.take(10).foreach(println)
    
    
    val cls = new LogisticRegressionWithSGD()
    cls.setIntercept(true)
    cls.optimizer.setNumIterations(200)
    
    def goal(a:Double):Double = {
	    cls.optimizer.setStepSize(a) 
	    
	    val means = for(i <-1 to 10 ) yield {
	      
	    val split = train.randomSplit(Array(0.75,0.25), i)  
	    val model = cls.run(split(0))
	    val pred = Ex.predictProb(model,split(1).map(_.features)).zip(split(1).map(_.label))    
	    Eval.logLogs(pred)
	      
	    }
	   val logLoss  = means.toList.reduce( _ + _)/means.size 
	   println("Mean log loss:" + logLoss + " for: " + a)
	   logLoss
    }
    
    val opa = optimize(1,200,0.0001)(goal)
    println("OptLearning Rate: " + opa)    
  
  * 
  */
    exit(0)
  }

}

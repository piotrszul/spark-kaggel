package ml

import df.DataFrame
import df.ColumnDef
import org.apache.spark.rdd.RDD
import df.NumericalSummary
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap
import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import df.Helpers._
import scala.collection.JavaConverters._


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
  def dropLevels(cols:Seq[ColumnDef], rdd:RDD[List[Any]],maps:Array[Long2LongOpenHashMap], minCount:Int):DataFrame = {
	  implicit val columns = cols
	  val mapsBdcast = rdd.context.broadcast(maps)
	  new DataFrame( cols, rdd.map(categorical { case (v,i) => val c = mapsBdcast.value(i).get(v); if (c>minCount) v else -1 }))    
  }

  def mapLevels(cols:Seq[ColumnDef], rdd:RDD[List[Any]],maps:Long2IntOpenHashMap):DataFrame = {
	  implicit val columns = cols
	  val mapsBdcast = rdd.context.broadcast(maps)
	  new DataFrame( cols, rdd.map(categorical { case (v,i) => mapsBdcast.value.get((i << 32L)|(v&0xffffffffL)) }))    
  }

  
  def logScaler(cols:Seq[ColumnDef], rdd:RDD[List[Any]],summary:Map[Int, NumericalSummary]):DataFrame = {
	  implicit val columns = cols
	  new DataFrame( cols, rdd.map(numerical { case (v,i) => val s = summary(i); math.log(v-s.min +1) }))    
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

  def levelCounts(cols:Seq[ColumnDef], rdd:RDD[List[Any]]):Array[Long2LongOpenHashMap] = {
      val result: Array[Long2LongOpenHashMap] = rdd.aggregate(Array.fill(cols.size)(new Long2LongOpenHashMap()))(
      { (s: Array[Long2LongOpenHashMap], v: List[Any]) =>
        v.zipWithIndex.filter(k => k._2 > 0 && cols(k._2).isCategorical)
          .foreach({ case (v, i) => 
            val vl = v.asInstanceOf[Long]
            s(i).add(vl,1)  });
      	  s
      }, combine)
      result
  }

  def buildIndex(cols:Seq[ColumnDef], rdd:RDD[List[Any]]):Array[LongOpenHashSet] = {
     val result: Array[LongOpenHashSet] = rdd.aggregate(Array.fill(cols.size)(new LongOpenHashSet()))(
      { (s: Array[LongOpenHashSet], v: List[Any]) =>
        v.zipWithIndex.filter(k => k._2> 0 && cols(k._2).isCategorical)
          .foreach({ case (v, i) => if (!cols(i).varType.isNaN(v)) s(i).add(v.asInstanceOf[Long]) }); s
      },
      { (s1: Array[LongOpenHashSet], s2: Array[LongOpenHashSet]) => s1.zip(s2).foreach(k => k._1.addAll(k._2)); s1 })
      result
  }

}

class FilterLevles(minCount:Int) extends Processor {
  var mapping:Array[Long2LongOpenHashMap] = null
  def fit(f: DataFrame) {
    mapping = XXX.levelCounts(f.cols, f.rdd)
  }
  def transform(df: DataFrame):DataFrame = {
    XXX.dropLevels(df.cols,df.rdd, mapping, minCount)
  }
}

class GlobalIndexer() extends Processor {
  var mapping:Long2IntOpenHashMap = null
  var size = 0
  def fit(f: DataFrame) {
    val index:Array[LongOpenHashSet] = XXX.buildIndex(f.cols, f.rdd)
    var idx:Int = 0
    mapping = new Long2IntOpenHashMap()
    mapping.defaultReturnValue(-1)
    index.zipWithIndex.filter(_._1.size() >0).foreach{ case (h,i) =>
      h.iterator().asScala.filter(_!= -1).foreach{v => mapping.put(((i << 32L)|(v&0xffffffffL)).asInstanceOf[java.lang.Long]
    		  ,idx.asInstanceOf[Integer]) ; idx+=1}
    }
    size = idx
  }
  
  def transform(df: DataFrame):DataFrame = {
    XXX.mapLevels(df.cols,df.rdd, mapping)
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

class LogScaler extends Processor {
 
  var  summary:Map[Int, NumericalSummary] = null
  
  def fit(df: DataFrame) = {
    summary = df.mins()
  }
  def transform(df: DataFrame):DataFrame = XXX.logScaler(df.cols, df.rdd, summary)
}


class Pipe(elements:Seq[Processor]) extends Processor {
  def fit(df: DataFrame) = {}//elements.foreach(_.fit(df)) 
  def transform(df: DataFrame):DataFrame = {
    var result = df
    elements.foreach{e => e.fit(result); result = e.transform(result)}
    result
  }
  
}

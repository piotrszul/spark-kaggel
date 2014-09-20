package df

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.reflect.ClassTag

import DataFrame._
import utils.Utils

case class NumSummary(var min:Double, var max:Double, var sum:Double, var count:Long) {
  def this(v: Double) = this(v,v,v,1L)
}


object Ops {
	def reduce(a:NumSummary,b:NumSummary):NumSummary = {
	  a.min = Math.min(a.min, b.min)
	  a.max  = Math.max(a.max, b.max)
	  a.sum += b.sum
	  a.count += b.count
	  a
	}
}

abstract class VarType[T] {
  def fromStr(s: String): T
  def NaN: T
  def isNaN(v: Any): Boolean = (NaN == v)
}

case class NumericalVarType extends VarType[Double] {
  override val NaN = Double.NaN
  override def fromStr(s: String): Double = if (!s.isEmpty()) s.toDouble else NaN
  override def isNaN(v: Any): Boolean = v.asInstanceOf[Double].isNaN()
}

case class CategoricalVarType extends VarType[Long] {
  override val NaN = -1L
  override def fromStr(s: String): Long = if (!s.isEmpty()) java.lang.Long.parseLong(s, 16) else NaN
}

object VarType {
  val Numerical = NumericalVarType()
  val Categorical = CategoricalVarType()
}

case class ColumnDef(name: String, varType: VarType[_]) {
	def isCategorical = VarType.Categorical.equals(varType)
}

abstract class VarSummary {
}

case class NumericalSummary(min:Double, max:Double, mean:Double, sum:Double, count:Long) extends VarSummary {
  def this(ns: NumSummary) = this(ns.min,ns.max,ns.sum/ns.count, ns.sum, ns.count)
}

case class CategoricalSummary(mode:Long, levels:Long) extends VarSummary 


case class Summary(cols:Seq[ColumnDef], summary:Seq[VarSummary]) {
  def print() {
  cols.zip(summary).foreach { case (c,f) =>
    println(c.name, c.varType, ":" , f)  
  }}
}

abstract class ColRef
case class IndexRef(index: Int) extends ColRef
case class NameRef(name: String) extends ColRef



class DataFrame(val cols: Seq[ColumnDef], val rdd: RDD[List[Any]]) {
  def map[T](f: List[Any] => T)(implicit ct: ClassTag[T]): RDD[T] = {
    rdd.map(f)
  }

  def project(p: Int => Boolean): DataFrame = {
    // it needs to come down to set projection
    new DataFrame(cols.zipWithIndex.filter(vi => p(vi._2)).map(_._1),
      rdd.map(r => r.zipWithIndex.filter(vi => p(vi._2)).map(_._1)))
  }
  def projectCols(p: ColumnDef => Boolean): DataFrame = project { i: Int => p(cols(i)) }
  def project(s: Set[Int]): DataFrame = project(s.contains _)
  def project(s: Set[String])(implicit tf: ClassTag[String]): DataFrame = projectCols { cd: ColumnDef => s.contains(cd.name) }

  def resolveRef(r: ColRef): Int = r match {
    case IndexRef(i) => i
    case NameRef(n) => cols.indexWhere(_.name.equals(n))
  }

  def toVW(label: ColRef, tag: ColRef): RDD[String] = DataFrame.toVW(rdd,resolveRef(label),resolveRef(tag), cols)
    
  def countLevels():Map[Int,Long]  = DataFrame.countLevelsInt(cols,rdd)
  def modes():Map[Int,(Any,Long)]  = DataFrame.modesInt(cols,rdd)
  def mins()  = DataFrame.minsInt(cols,rdd)
  
  def summary():Summary = {
    val levels = countLevels()
    val mo = modes()
    val mi= mins()
    
    return new Summary(cols,
        cols.zipWithIndex.map{ case (v,i) => v.varType match {
          		case NumericalVarType() => mi(i)
          		case CategoricalVarType() => CategoricalSummary(mo(i)._1.asInstanceOf[Long], levels(i))
        	}
        }
    )
  }
}

object DataFrame {
  
  def toVW(rdd: RDD[List[Any]], labelIndex:Int,tagIndex:Int,cols: Seq[ColumnDef]) = rdd.map(DataFrame.rowToVW(labelIndex,tagIndex, cols))
  
  def rowToVW(labelIndex:Int,tagIndex:Int,cols: Seq[ColumnDef])(d:Seq[Any]):String  = {
      "" + (if (d(labelIndex).asInstanceOf[Double]  > 0) 1.0 else -1.0 ) + " '" + d(tagIndex).asInstanceOf[Long].toHexString + " " +
        "|Num " + 
      	d.zipWithIndex
        .filter { case (v, i) => i != labelIndex && i != tagIndex }
        .map { case (v, i) => (v, cols(i)) }
        .filter { case (v, c) => !c.varType.isNaN(v) && !c.isCategorical}
        .map {
          case (v, c) => c.varType match {
            case NumericalVarType() => c.name + ":" + v
          }
        }.mkString(" ") + " |Cats " + 
        d.zipWithIndex
        .filter { case (v, i) => i != labelIndex && i != tagIndex }
        .map { case (v, i) => (v, cols(i)) }
        .filter { case (v, c) => !c.varType.isNaN(v) && c.isCategorical}
        .map {
          case (v, c) => c.varType match {
            case CategoricalVarType() => "_" + v 
          }
        }.mkString(" ") 

  //            case CategoricalVarType() => "|" + c.name + " _" + v.asInstanceOf[Long].toHexString

  }
  
  def countLevelsInt(cols: Seq[ColumnDef], rdd: RDD[List[Any]]):Map[Int,Long]  = {
    rdd.flatMap{ d =>
		d.zip(cols.zipWithIndex)
			.filter{ case (v, (c,i)) => c.isCategorical}
			.filter{ case (v, (c,i)) => !c.varType.isNaN(v)}
			.map {case (v, (c,i)) => (i,v) }
    }.distinct.countByKey.toMap    
  }

  def modesInt(cols: Seq[ColumnDef], rdd: RDD[List[Any]]):Map[Int,(Any,Long)]  = {
    rdd.flatMap{ d =>
		d.zip(cols.zipWithIndex)
			.filter{ case (v, (c,i)) => c.isCategorical}
			.filter{ case (v, (c,i)) => !c.varType.isNaN(v)}
			.map {case (v, (c,i)) => ((i,v),1L) }
    }.reduceByKey(_+_)
    .map({case ((i,v), count) => (i, (v,count))})
    .reduceByKey((t1:(Any,Long),t2:(Any,Long))=> if (t1._2 > t2._2) t1 else t2).collectAsMap.toMap  
  }


  def minsInt(cols: Seq[ColumnDef], rdd: RDD[List[Any]]):Map[Int,NumericalSummary]  = {
    rdd.flatMap{ d =>
		d.zip(cols.zipWithIndex)
			.filter{ case (v, (c,i)) => !c.isCategorical}
			.filter{ case (v, (c,i)) => !c.varType.isNaN(v)}
			.map {case (v, (c,i)) => (i,new NumSummary(v.asInstanceOf[Double])) }
    }.reduceByKey(Ops.reduce).map(t => (t._1, new NumericalSummary(t._2))).collectAsMap.toMap 
  }

  
  def fromCSV(rdd: RDD[String])(typeGuesser: (Int, String) => VarType[_]): DataFrame = {
    val strHeader = rdd.first().split(",")
    val dfHeader = strHeader.zipWithIndex.map(t => ColumnDef(t._1, typeGuesser(t._2, t._1)))
    val dfRDD = rdd.filter(!_.startsWith("Id")).map(l => l.split(",").toList.padTo(dfHeader.length,""))
      .map(a => a.zip(dfHeader).map(t => t._2.varType.fromStr(t._1.trim())).toList)
    new DataFrame(dfHeader, dfRDD)
  }

  implicit def intToColRef(i: Int): ColRef = IndexRef(i)
  implicit def stringToColRef(n: String): ColRef = NameRef(n)
}

object Imputer {
  def predict(cols: Seq[ColumnDef], rdd: RDD[List[Any]], summary:Summary):DataFrame = {
    	return new DataFrame(cols, 
	    rdd.map{ r =>
	    	r.zipWithIndex.map{ case (v,i) => if (!cols(i).varType.isNaN(v)) v else summary.summary(i) match {
	    	  case CategoricalSummary(mode,_) => mode.asInstanceOf[Any]
	    	  case NumericalSummary(_,_,mean,_,_) => mean.asInstanceOf[Any]
	    	}
	      }
		}
	 )	  
  }
}

class Imputer {
  var summary:Summary = null
  def fit(df:DataFrame) {
    summary = df.summary()
  }
  def predict(df:DataFrame):DataFrame = Imputer.predict(df.cols, df.rdd,summary)
}

trait AbstractRow 

class Row(val l:List[Any]) extends AnyVal {
	def getLong(i:Int):Long = l(i).asInstanceOf[Long]
	def getDouble(i:Int):Double = l(i).asInstanceOf[Double]
}

object DataFrameApp {

  implicit def l2Row(l:List[Any])=new  Row(l)
  
  def main(args: Array[String]) = {

    val sc = new SparkContext(new SparkConf().setAppName("KaggelApp"))
    val df = DataFrame.fromCSV(sc.textFile(args(0))) {
      case (i, n) =>
        if (n.startsWith("C") || i == 0) VarType.Categorical else VarType.Numerical
    };

    
    val summary:Summary = if (args.length >2) Utils.readObject(args(2)) else df.summary()
    
    val rdd = df.rdd.map{l =>
    	l.zipWithIndex.map{ case (v, i) =>
    		if (i>1 && i< 15) {
    		  val s = summary.summary(i).asInstanceOf[NumericalSummary]
    		  val dff = v.asInstanceOf[Double]
    		  val df = if (!dff.isNaN()) dff else 0.0
    		  val mi = math.log(1) 
    		  val ma  = math.log(s.max - s.min +1)
    		  (math.log(df-s.min + 1)-mi)/(ma-mi)    		  
    		} else v;
    	}
    } 
    
    val dff = new DataFrame(df.cols, rdd)
/*
    val summary = df.summary()    
    summary.print()
    
    val imputer = new Imputer()
    imputer.summary = summary
    //imputer.fit(df)
    //val idf = imputer.predict(df)
   
    
    val kidf = df.project(i => (i < 2) || (summary.summary(i) match {
      case CategoricalSummary(_,level) => level < args(2).toInt
      case _ => true
    }))
 */   
    
    dff.toVW("Label","Id").coalesce(1,true).saveAsTextFile(args(1))
 /*   
    val levels = df.countLevels()
    val projDF = df.project(i => levels.get(i).isEmpty || levels(i) < 1000)
    println(projDF.cols)
    println(projDF.countLevels())
*/
  }

}

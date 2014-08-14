
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.io.ObjectOutputStream
import java.io.FileOutputStream

object KaggelDict {
  
  def add(t1:(Long,Long), t2:(Long,Long)):(Long,Long) = (t1._1 + t2._1, t1._2 + t2._2)
  def addd(t1:(Long,Double), t2:(Long,Double)):(Long,Double) = (t1._1 + t2._1, t1._2 + t2._2)
  
  def main(args: Array[String]) {

    //java.lang.Long.parseLong("891b62e7", 16)
    
    val sc = new SparkContext(new SparkConf().setAppName("KaggelApp"))
    val in = sc.textFile(args(0))
    val input  = in.map(_.split(","))
    val header = input.first().toList
    //println(header)
    val data = input.filter(t => !t(0).startsWith("Id"))
    //data.take(10).foreach(println)
    
    val df = data.map{t => 
      val padded = t.iterator.padTo(header.length, "")
      val vals = header.iterator.zip(padded)
      vals.map{ case (col, str) => 
        if (str.isEmpty()) null else if (col.startsWith("C")) java.lang.Long.parseLong(str, 16) else str.toInt 
      }.toList
    }
    
//    df.first.foreach(v => println(v.getClass))
    val ff = df.flatMap{ t => t.zipWithIndex.zip(header).filter(_._2.startsWith("C")).filter(_._1._1 != null)
      .map(vi => (vi._1.swap, (1L, t(1).asInstanceOf[Long])))
    }
    val rf = ff.reduceByKey(add).map {case (k, (t, p)) => (k, p.toDouble/t)}.cache()
     
    rf.map(t => ""+ t._1._1 + "," +  t._1._2 + "," + t._2).saveAsTextFile(args(1))
    val map = rf.collect().toMap
    
    val out = new ObjectOutputStream(new FileOutputStream("mapping.obj"))
    out.writeObject(map)
    out.close()
  }
} 

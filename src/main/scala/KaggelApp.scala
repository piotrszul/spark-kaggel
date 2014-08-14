
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.io.FileInputStream
import java.io.ObjectInputStream

object KaggelApp {
  
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
    
    val mapin = new ObjectInputStream(new FileInputStream("mapping.obj"))
    val encooding = mapin.readObject().asInstanceOf[Map[(Int,Any), Double]]   
    mapin.close();
    val encoding = sc.broadcast(encooding)
    val dff = df.map{v =>
    	v.zipWithIndex.map{ t=> if (t._1 == null) null else
    	  if (encoding.value.get(t.swap).isDefined) encoding.value.get(t.swap).get else t._1.asInstanceOf[Long].toDouble}
    }
    //dff.take(10).foreach(println)
    // get all averages
    dff.map(_.mkString(",")).saveAsTextFile(args(1))    

/*    
    val meansRDD = dff.flatMap{ r => r.zipWithIndex.filter(_._1!=null).map{ case (v,i) => (i, (1L, v.asInstanceOf[Double]))}}
    .reduceByKey(addd).map {case (k, (t, p)) => (k, p.toDouble/t)}
    
    val means = meansRDD.collect().toMap
    
    val dfff = dff.map {r =>
    	r.zipWithIndex.map{ case (v,i) => if (v!=null) v else means.get(i).get}
    }
    
    //dfff.take(10).foreach(println)
    
    dfff.map(_.mkString(",")).saveAsTextFile(args(1))    
  
  * 
  */
    }
} 

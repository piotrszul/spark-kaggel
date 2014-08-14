
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.io.FileInputStream
import java.io.ObjectInputStream

object KaggelNa {
  
  def add(t1:(Long,Long), t2:(Long,Long)):(Long,Long) = (t1._1 + t2._1, t1._2 + t2._2)
  def addd(t1:(Long,Double), t2:(Long,Double)):(Long,Double) = (t1._1 + t2._1, t1._2 + t2._2)
  
  def main(args: Array[String]) {

    //java.lang.Long.parseLong("891b62e7", 16)
    
    val sc = new SparkContext(new SparkConf().setAppName("KaggelNa"))
    val in = sc.textFile(args(0))
    val dff  = in.map(_.split(",")).map(_.map(s => if ("null".equals(s)) Double.NaN else s.toDouble))

    val meansRDD = dff.flatMap{ r => r.zipWithIndex.filter(!_._1.isNaN()).map{ case (v,i) => (i, (1L, v.asInstanceOf[Double]))}}
    .reduceByKey(addd).map {case (k, (t, p)) => (k, p.toDouble/t)}
    
    val means = meansRDD.collect().toMap
    
    val dfff = dff.map {r =>
    	r.zipWithIndex.map{ case (v,i) => if (!v.isNaN()) v else means.get(i).get}
    }
    
    //dfff.take(10).foreach(println)
    
    dfff.map(_.mkString(",")).saveAsTextFile(args(1))      
    }
} 

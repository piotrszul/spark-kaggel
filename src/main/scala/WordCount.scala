
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object WordCount {
  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("WordCount").setMaster("local"))
    val in = sc.parallelize(List.range(0,100), 10)
    val rdd = in.zipWithIndex.map(_.swap).groupByKey
    println(rdd.partitions.toList)
    println(rdd)
    rdd.collect().foreach(println)
  }
} 

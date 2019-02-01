package comp9313.ass3
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object Problem2 {
    def main(args: Array[String]) {
      val inputFile = args(0)
      val outputFolder = args(1)
      val conf = new SparkConf().setAppName("ReverseGraph").setMaster("local")
      val sc = new SparkContext(conf)
      val input = sc.textFile(inputFile)
      val reversedEdges = input.map(line => line.split("\t")).map(x => (x(0), x(1))).map(x => x.swap)
      val nodeList = reversedEdges.map(x => (x._1.toInt, x._2.toInt)).groupByKey.map(x => (x._1, x._2.toList.sortWith(_<_))).sortByKey(true)
      val outputFormat = nodeList.map(x => x._1 +"\t" + x._2.mkString(","))
      outputFormat.saveAsTextFile(outputFolder)
    }
}
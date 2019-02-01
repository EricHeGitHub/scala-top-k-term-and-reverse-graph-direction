package comp9313.ass3
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object Problem1 {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFolder = args(1)
    val k = args(2).toInt
    val conf = new SparkConf().setAppName("WordCountbyLine").setMaster("local")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile).map ( x => x.toLowerCase )
    val words = input.flatMap(line => line.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+").distinct).map ( x => (x, 1) )
    
    val filteredWords = words.filter(x => x._1.length >= 1 && x._1.charAt(0) >= 'a' && x._1.charAt(0) <= 'z')
    val wordCountbyLine = filteredWords.reduceByKey(_+_).map(x => x.swap).sortBy(x=>(-x._1, x._2), true).map(x => x.swap).take(k).map(x => x._1 +'\t'+ x._2)
    val output = sc.parallelize(wordCountbyLine)
    output.saveAsTextFile(outputFolder)
    
  }
}
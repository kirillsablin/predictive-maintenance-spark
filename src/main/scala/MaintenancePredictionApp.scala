import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

object MaintenancePredictionApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    DStream
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(w => (w, 1))
    val wCounts = pairs.reduceByKey(_+_)
    wCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

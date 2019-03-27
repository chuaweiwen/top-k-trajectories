import java.io.PrintWriter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import annotation.tailrec
import scala.reflect.ClassTag

import java.util.Date
import java.text.SimpleDateFormat

case class DataPoint(driverID: String, orderID: String, hour: Int, minute: Int, longitude: Float, latitude: Float) extends Serializable

object Main extends Main {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Top-k_Trajectories")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val interestLongitude = 0f
    val interestLatitude = 0f
    val startTime = 0
    val endTime = 0

    val lines = sc.textFile("data/input/didi_sample_data")
    val dataPoints = rawDataPoints(lines)

    dataPoints.saveAsTextFile("data/out/test.txt")
  }
}

class Main extends Serializable {

  /** Load data points from the given file */
  def rawDataPoints(lines: RDD[String]): RDD[DataPoint] = {
    val hourDf:SimpleDateFormat = new SimpleDateFormat("HH")
    val minuteDf:SimpleDateFormat = new SimpleDateFormat("mm")
    lines.map(line => {
      val arr = line.split(",")
      DataPoint(
        driverID = arr(0),
        orderID = arr(1),
        hour = hourDf.format(arr(2).toLong).toInt,
        minute = minuteDf.format(arr(2).toLong).toInt,
        longitude = arr(3).toFloat,
        latitude = arr(4).toFloat)
    })
  }
}

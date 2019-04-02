import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import java.text.SimpleDateFormat

case class DataPoint(driverID: String, orderID: String, minute: Int, longitude: Float, latitude: Float) extends Serializable

object Main extends Main {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Top-k_Trajectories")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    val querypt = sc.textFile("data/input/query")
    val test = querypt.map(line => {
      val arr = line.split(",")
      (arr(0).toFloat, arr(1).toFloat,arr(2).toInt,arr(3).toInt,arr(4).toInt,arr(5).toInt,arr(6).toInt)
    })
    val k = test.collect().apply(0)._3
    val startHr = test.collect().apply(0)._4
    val startMin = test.collect().apply(0)._5
    val endHr = test.collect().apply(0)._6
    val endMin = test.collect().apply(0)._7

    val start = startHr * 60 + startMin
    val end = endHr * 60 + endMin

    val interestLongitude = test.collect().apply(0)._1
    val interestLatitude = test.collect().apply(0)._2

    val lines = getLines(sc)
    val dataPoints = rawDataPoints(lines)
    val trajectories = getTrajectories(dataPoints)
    val filteredTrajectories = filterTrajectoriesByTime(trajectories, start, end)
    val trajectoriesWithDistance = appendMinimumDistance(filteredTrajectories, interestLongitude, interestLatitude)
    val result = sc.parallelize(trajectoriesWithDistance.take(k)).map(x => (x._1, x._3))
    result.saveAsTextFile("data/out/result3.txt")
  }
}

class Main extends Serializable {

  final val MAX_MINUTE = 1439

  def getLines(sc : SparkContext) : RDD[String] = {
    sc.textFile("gps_20161001.csv")
      .union(sc.textFile("gps_20161002.csv"))
      .union(sc.textFile("gps_20161003.csv"))
      .union(sc.textFile("gps_20161004.csv"))
      .union(sc.textFile("gps_20161005.csv"))
      .union(sc.textFile("gps_20161006.csv"))
      .union(sc.textFile("gps_20161007.csv"))
      .union(sc.textFile("gps_20161008.csv"))
      .union(sc.textFile("gps_20161009.csv"))
      .union(sc.textFile("gps_20161010.csv"))
      .union(sc.textFile("gps_20161011.csv"))
      .union(sc.textFile("gps_20161012.csv"))
      .union(sc.textFile("gps_20161013.csv"))
      .union(sc.textFile("gps_20161014.csv"))
      .union(sc.textFile("gps_20161015.csv"))
      .union(sc.textFile("gps_20161016.csv"))
      .union(sc.textFile("gps_20161017.csv"))
      .union(sc.textFile("gps_20161018.csv"))
      .union(sc.textFile("gps_20161019.csv"))
      .union(sc.textFile("gps_20161020.csv"))
      .union(sc.textFile("gps_20161021.csv"))
      .union(sc.textFile("gps_20161022.csv"))
      .union(sc.textFile("gps_20161023.csv"))
      .union(sc.textFile("gps_20161024.csv"))
      .union(sc.textFile("gps_20161025.csv"))
      .union(sc.textFile("gps_20161026.csv"))
      .union(sc.textFile("gps_20161027.csv"))
      .union(sc.textFile("gps_20161028.csv"))
      .union(sc.textFile("gps_20161029.csv"))
      .union(sc.textFile("gps_20161030.csv"))
      .union(sc.textFile("gps_20161031.csv"))
  }

  /** Load data points from the given file */
  def rawDataPoints(lines: RDD[String]): RDD[DataPoint] = {
    val hourDf:SimpleDateFormat = new SimpleDateFormat("HH")
    val minuteDf:SimpleDateFormat = new SimpleDateFormat("mm")
    lines.map(line => {
      val arr = line.split(",")
      DataPoint(
        driverID = arr(0),
        orderID = arr(1),
        minute = (hourDf.format(arr(2).toLong * 1000L).toInt * 60) + minuteDf.format(arr(2).toLong * 1000L).toInt,
        longitude = arr(3).toFloat,
        latitude = arr(4).toFloat)
    })
  }

  /** Group the trajectories together */
  def getTrajectories(dataPoints: RDD[DataPoint]): RDD[(String, Iterable[(String, Int, Float, Float)])] = {
    dataPoints.map(dp => (dp.orderID, dp.minute, dp.longitude, dp.latitude)).groupBy(_._1)
  }

  /** Filter the trajectories based on user input time */
  def filterTrajectoriesByTime(dataPoints: RDD[(String, Iterable[(String, Int, Float, Float)])], start: Int, end: Int):
    RDD[(String, Iterable[(Float, Float)])] = {
    dataPoints.filter { case (key, xs) =>
      xs.exists { xss =>
        if (start <= end) {
          (xss._2 >= start) && (xss._2 <= end)
        } else {
          (xss._2 >= start) && (xss._2 <= MAX_MINUTE) || (xss._2 >= 0) && (xss._2 <= end)
        }
      }
    }.map(x => (x._1, x._2.map(x => (x._3, x._4))))
  }

  /** Return trajectories with minimum distance to query point sorted by distance in ascending order **/
  def appendMinimumDistance(trajectory: RDD[(String, Iterable[(Float, Float)])], queryLong: Float, queryLat: Float) :
    RDD[(String, Double, Iterable[(Float, Float)])] = {
    trajectory.map(x => (x._1, x._2.map(
      x => euclideanDistance(x._1, x._2, queryLong, queryLat))
      .reduce((x,y) => List(x,y).min), x._2)).sortBy(_._2)
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(x1: Float, y1: Float, x2: Float, y2: Float): Double = {
    Math.sqrt(Math.pow(x2 - x1,2) +  Math.pow(y2 - y1,2))
  }
}

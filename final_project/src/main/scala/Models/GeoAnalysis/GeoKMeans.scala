package Models.GeoAnalysis
import org.apache.spark.ml.linalg.SQLDataTypes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

/**
  * Created by kple on 12/2/16.
  */
object GeoKMeans {
  def runKMeans(data: RDD[String]): Unit = {
    import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
    import org.apache.spark.mllib.linalg.Vectors

    val points = data.map(line => {
//      println(line)
      val cols = line.substring(1, line.length-1).split(",").tail
      Vectors.dense(cols.map(_.toDouble))
    })
    // Load and parse the data
    // Cluster the data into two classes using KMeans
    val numClusters = 20
    val numIterations = 10
    val clusters = KMeans.train(points, numClusters, numIterations)

    clusters.clusterCenters.map(println)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(points)
    println("Within Set Sum of Squared Errors = " + WSSSE)
  }
}

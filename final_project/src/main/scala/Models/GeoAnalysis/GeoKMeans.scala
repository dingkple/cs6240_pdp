package Models.GeoAnalysis

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by kple on 12/2/16.
  */
class GeoKMeans {
  def runKMeans(data: RDD[String], sc: SparkContext, fileSystem: FileSystem): Unit = {


    val points = data.map(line => {
//      println(line)
      val cols = line.substring(1, line.length-1).split(",").tail
      Vectors.dense(cols.map(_.toDouble))
    })
    getModel(points, fileSystem, sc)
  }

  def getGeoVector(row: Array[String]): Vector = {
    Vectors.dense(Array(row(2).toDouble, row(3).toDouble))
  }

  def getVectorRDD(data: RDD[Array[String]]): RDD[Vector] = {
    data.map(row => {
      getGeoVector(row)
    })
  }

  def getModel(points: RDD[Vector], fileSystem: FileSystem, sc: SparkContext): KMeansModel = {
    // Load and parse the data
    // Cluster the data into two classes using KMeans
    val numClusters = 100
    val numIterations = 15
    val clusters = KMeans.train(points, numClusters, numIterations)
//
//    val prediction = points.map(p => {
//      (p, clusters.predict(p))
//    })
//
//    prediction.foreach(println(_))

//    Utils.Utils.checkoutput(fileSystem, Utils.Config.KMEANSOUTPUT)
//    clusters.save(sc, Utils.Config.KMEANSOUTPUT)


    // Evaluate clustering by computing Within Set Sum of Squared Errors
//    val WSSSE = clusters.computeCost(points)
//    println("Within Set Sum of Squared Errors = " + WSSSE)
    clusters
  }
}

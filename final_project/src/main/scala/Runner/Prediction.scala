package Runner

import java.net.URI

import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.storage.StorageLevel

/**
  * Created by kple on 12/9/16.
  */
object Prediction {

  def main(args: Array[String]): Unit = {
    val unlabeledFile = args(0)
    val modelPath = args(1)
    val output = args(2)
    val defaultParallelism = args(3).toInt * 3

    val conf = new SparkConf()
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val fileSystem = org.apache.hadoop.fs.FileSystem.get(new URI(output), hadoopConf)
    conf.setMaster("local[*]")
    conf.setAppName("final_project")
    conf.set("spark.default.parallelism", defaultParallelism.toString)
    val sc = new SparkContext(conf)

    if (fileSystem.exists(new Path(output))) fileSystem.delete(new Path(output), true)

    val model = GradientBoostedTreesModel.load(sc, modelPath)

    // Generate the prediction for unlabeled data
    val transformedTest = sc.textFile(unlabeledFile, defaultParallelism)
      .map(row => {
        val cols = row.split(",")
        if (!cols(0).startsWith("SAMPLING_EVENT_ID")) {
          val transform = new Transform
          val (id, label, features) = transform.transFormLine(cols)
          (id, features)
        } else {
          (Array("SAMPLE_ID", "PREDICTION").mkString(","), Vectors.dense(Array.emptyDoubleArray))
        }
      })
      .persist(StorageLevel.MEMORY_AND_DISK)

    transformedTest.map(rec => {
      if (rec._1.startsWith("SAMPLE_ID")) {
        rec._1
      } else {
        val predicted = model.predict(rec._2)
        rec._1 + "," + predicted
      }
    }).coalesce(1).saveAsTextFile(output + "/DingKadam.csv")
  }
}

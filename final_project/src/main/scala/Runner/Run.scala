package Runner

import java.net.URI

import Models.GeoAnalysis.GeoKMeans
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{GradientBoostedTrees, RandomForest}
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel, RandomForestModel}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

class Transform {
  /**
    * Transform an array of string to (sampleId, label, features), categorical and numerical features are handled,
    * all missing data is ignore.
    * @param cols
    * @return (String, Double, Vector) namely SampleId, label, features are returned, "features" is a sparse vector
    */
  def transFormLine(cols: Array[String])
  : (String, Double, Vector) = {

    val config = Utils.Config
    var sampleID = ""
    val indiceBuffer = new ListBuffer[Int]
    val valueBuffer = new ListBuffer[Double]
    var label = 0.0
    val geoTranslator = new GeoKMeans

    for (i <- 0 until config.NUM_OF_FEATURE) {
      sampleID = cols(0)

      if (!config.skip.contains(i + 1)) {
        if (config.geo.contains(i)) {
          if (i == config.geo(0)) {
            if (cols(config.geo(0)) != "?" && cols(config.geo(1)) != "?") {
              indiceBuffer += i
              valueBuffer += cols(2).toDouble
              indiceBuffer += i + 1
              valueBuffer += cols(3).toDouble
            }
          }
        } else if (config.cate.contains(i + 1)) {
          /*
          Ignore all missing values
           */
          if (cols(i) != "?") {
            val value = cols(i).toDouble
            indiceBuffer += i
            valueBuffer += value
          }
        } else {
          if (cols(i) != "?") {
            var value = 0.0
            if (cols(i) != "X") value = cols(i).toDouble
            else value = 1
            if (i + 1 == config.labelIdx) {
              if (value > 0)
                label = 1.0
            } else {
              if (value != 0) {
                indiceBuffer += i
                valueBuffer += value
              }
            }
          }
        }
      }
    }
    (sampleID, label, Vectors.sparse(config.NUM_OF_FEATURE, indiceBuffer.toArray[Int], valueBuffer.toArray[Double]))
  }

}

/**
  * Created by kple on 12/5/16.
  */
object Run {


  /**
    * Use Gradient Boosted Tree to train the data. Model and accuracy on the validation data is returned
    * @param training training data
    * @param test validation data
    * @param numOfIteration ie, number of trees
    * @param maxDepth max depth of each tree
    * @param learningRate use this to tune the speed of model adapting to the data
    * @param maxBin max bins for splitting
    * @return Maximum number of bins used for discretizing continuous features and for choosing how to split on
    *         features at each node. More bins give higher granularity.
    */
  def GBT(training: RDD[LabeledPoint], test: RDD[LabeledPoint], numOfIteration: Int = 50, maxDepth: Int = 6,
          learningRate: Double = 0.1, maxBin: Int = 32)
  : (GradientBoostedTreesModel, Double) = {
    // Train a GradientBoostedTrees model.
    // The defaultParams for Classification use LogLoss by default.
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")

    boostingStrategy.setNumIterations(numOfIteration) // Note: Use more iterations in practice.
    boostingStrategy.getTreeStrategy.setNumClasses(2)
    boostingStrategy.getTreeStrategy.setMaxDepth(maxDepth)
    boostingStrategy.getTreeStrategy.setMaxBins(maxBin)
    boostingStrategy.setLearningRate(learningRate)

    val model = GradientBoostedTrees.train(training, boostingStrategy)

    // Evaluate model on test instances and compute test error
    val labelAndPredictions = test.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val acc = labelAndPredictions.filter(r => r._1 == r._2).count.toDouble / test.count()
    println("ACC = " + acc)
    (model, acc)
  }

  /**
    * RandomForest Model to train the data
    * @param trainingData
    * @param testData
    * @param treeNum
    * @param maxDepth
    * @param maxBins
    * @return
    */
  def randomForest(trainingData: RDD[LabeledPoint], testData: RDD[LabeledPoint], treeNum: Int, maxDepth: Int = 10,
                   maxBins: Int = 100): (RandomForestModel, Double) = {

    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    // Use more in practice.
    val featureSubsetStrategy = "auto"
    // Let the algorithm choose.
    val impurity = "gini"

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      treeNum, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    //    labelAndPreds.saveAsTextFile("RF_result")
    val acc = labelAndPreds.filter(r => r._1 == r._2).count.toDouble / testData.count()
    println("ACC = " + acc)
    //    println("Learned classification forest model:\n" + model.toDebugString)
    (model, acc)
  }

  /*
  Write data to file
   */
  def writeAccTo(s: String, fileSystem: FileSystem, acc: String) = {
    val f = fileSystem.create(new Path(s))
    f.writeUTF("acc: " + acc)
    f.close()
  }

  /*
  Write data to file, all the turning data.
   */
  def writeTuneResult(path: String, fileSystem: FileSystem, rst:mutable.HashMap[Double, (Int, Int, Int)]): Unit = {
    val f = fileSystem.create(new Path(path))
    for (k <- rst.keys) {
      f.writeUTF(k + " " + rst(k).toString() + "\n")
    }
    f.close()
  }


  /**
    * Tuning the parameter of GBT, onlly maxBins, maxDepth, maxIteration is consider here, the strategy of find the
    * parameter to test is random choosing. For each parameter, choose a possible value from the given range. Then
    * check if this set of parameter is already tested, if yes, generate next set, if not, test it and save the model
    * and result. In the end, select the best one and write all the results to disk
    * @param trainingData
    * @param testData
    * @param iteration
    * @param sc
    * @param baseDir
    * @param fileSystem
    * @return best accuracy and the path to best model
    */
  def tuneParameters(trainingData: RDD[LabeledPoint], testData: RDD[LabeledPoint], iteration: Int,
                     sc: SparkContext, baseDir: String, fileSystem: FileSystem): (Double, String) = {

//    val learningRate = (0.001, 0.01, 0.1, 0.2)
    val maxBin = Array(32, 64)
    val maxDepth = Array(2,4,6)
    val numberOfIteration = Array(10, 15, 20)

    var rst:mutable.HashMap[Double, (Int, Int, Int)] = new mutable.HashMap[Double, (Int, Int, Int)]

    var i = 0
    while (i < iteration) {
      val rng = new Random
      rng.setSeed(System.nanoTime())
      val mb = maxBin(Math.abs(rng.nextInt() % maxBin.length))
      val md = maxDepth(Math.abs(rng.nextInt() % maxBin.length))
      val iter = numberOfIteration(Math.abs(rng.nextInt() % numberOfIteration.length))
      val strPath = baseDir + mb + "_" + md + "_" + iter
      if (!fileSystem.exists(new Path(strPath))) {
        val (model, acc) = GBT(trainingData, testData, maxDepth = md, numOfIteration = iter, maxBin = mb)
        model.save(sc, strPath)
        rst += acc -> (mb, md, iter)
        i += 1
      }
    }
    val best = rst.keySet.maxBy(k => k)
    val (mb, md, iter) = rst(best)
    println("Best Tune ACC: " + mb + " " + md + " " + iter)
    writeTuneResult(baseDir + "_tune_result", fileSystem, rst)
    (best, baseDir + mb + "_" + md + "_" + iter)
  }


  def main(args: Array[String]): Unit = {
    val inputFile = args(0)
    val testFile = args(1)
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

    val labelIdx = Utils.Config.labelIdx
    val numOfPartition = defaultParallelism * 2
    println(numOfPartition)

    val transformedInput = sc.textFile(inputFile, numOfPartition)
      .map(row => {
        val cols = row.split(",")
        val transform = new Transform

        if (!cols(0).startsWith("SAMPLING_EVENT_ID") && cols(labelIdx - 1) != "?") {
          val (id, label, features) = transform.transFormLine(cols)
          LabeledPoint(label, features)
        }
        else {
          LabeledPoint(-1, Vectors.dense(Array.emptyDoubleArray))
        }
      })
      .persist(StorageLevel.MEMORY_AND_DISK)
      .filter(_.label != -1)


    val seed = 435345L
    val shouldKFold = false
    val kFold = 5
    val tune = false

    val accBuffer = new ListBuffer[Double]
    var best = 0.0
    var modelPath = ""

    // Use K-fold to get the average and stddev of the training result.
    if (shouldKFold) {
      val kFoldArray = MLUtils.kFold(transformedInput, kFold, seed)
      transformedInput.unpersist()

      for (i <- 0 until kFold) {
        val (trainingData: RDD[LabeledPoint], testData: RDD[LabeledPoint])
        = (
          kFoldArray(i)._1.persist(StorageLevel.MEMORY_AND_DISK),
          kFoldArray(i)._2.persist(StorageLevel.MEMORY_AND_DISK)
        )

        val (model, acc) = GBT(trainingData, testData, maxDepth = 6, numOfIteration = 100)

        model.save(sc, output + "/GBT_model" + i)
        accBuffer += acc

        trainingData.unpersist()
        testData.unpersist()
      }
      best = accBuffer.maxBy(k => k)
    } else {

      // K-fold is too expensive, so for some parameters, only run once.
      val splits = transformedInput.randomSplit(Array(0.7, 0.3))
      val (trainingData, testData)
      = (splits(0).persist(StorageLevel.MEMORY_AND_DISK), splits(1).persist(StorageLevel.MEMORY_AND_DISK))

      if (tune) {
        val tuneRst = tuneParameters(trainingData, testData, 6, sc, output + "/GBT_model", fileSystem)
        modelPath = tuneRst._2
      } else {
        val (model, acc) = GBT(trainingData, testData, numOfIteration = 100, maxDepth = 7)
        model.save(sc, output + "/GBT_model")

        trainingData.unpersist()
        testData.unpersist()
        writeAccTo(output + "/validation_acc", fileSystem, acc.toString)
      }
    }

    val algorithmStr = "GBT_model"

    /*
    Save K-Fold result to file
     */
    if (shouldKFold) {
      modelPath = output + "/" + algorithmStr + accBuffer.indexOf(best)

      val count = accBuffer.size
      val mean = accBuffer.sum / count
      val devs = accBuffer.map(score => (score - mean) * (score - mean))
      val stddev = Math.sqrt(devs.sum / (count - 1))

      writeAccTo(output + "/validation_acc", fileSystem, Array(mean, stddev, count, devs)
        .foldLeft("")((left, num) => left + " " + num.toString))
    } else {
      if (!tune) {
        modelPath = output + "/" + algorithmStr
      }
    }

    val model = GradientBoostedTreesModel.load(sc, modelPath)
    if (!fileSystem.exists(new Path(output + "/GBT_model"))) model.save(sc, output + "/GBT_model")

  }
}

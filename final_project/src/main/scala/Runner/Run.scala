package Runner

import java.net.URI

import Models.GeoAnalysis.GeoKMeans
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{GradientBoostedTrees, RandomForest}
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel, RandomForestModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by kple on 12/5/16.
  */
object Run {

  def extractAllPoints(rDD: RDD[String]): Unit = {

  }

  def transFormLine(cols: Array[String], kMeansModel: KMeansModel): (String, Double, Vector) = {

    val config = Utils.Config
    var sampleID = ""
    val indiceBuffer = new ListBuffer[Int]
    val valueBuffer = new ListBuffer[Double]
    var label = 0.0
    val geoTranslator = new GeoKMeans

    val useKMeans = true
    for (i <- 0 until config.NUM_OF_FEATURE) {
      sampleID = cols(0)

      if (!config.skip.contains(i + 1)) {
        if (config.geo.contains(i)) {
          if (i == config.geo(0)) {
            if (cols(config.geo(0)) != "?" && cols(config.geo(1)) != "?") {
              val vec = geoTranslator.getGeoVector(cols)
              if (useKMeans) {
                indiceBuffer += i
                valueBuffer += kMeansModel.predict(vec)
                println(vec + " " + kMeansModel.predict(vec))
              } else {
                indiceBuffer += i
                valueBuffer += cols(2).toDouble

                indiceBuffer += i + 1
                valueBuffer += cols(3).toDouble
              }
            }
          }
        } else if (config.cate.contains(i + 1)) {
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

  def transformToLabledPointsWithLabel(input: RDD[Array[String]], fileSystem: FileSystem, sc: SparkContext,
                                       kMeansModel: KMeansModel):
  RDD[(String, LabeledPoint)] = {

    val transformed = input.map(row => {
      val (id, label, features) = transFormLine(row, kMeansModel)
      (id, LabeledPoint(label, features))
    })
    transformed
  }

  def transformToLabledPoints(input: RDD[Array[String]], fileSystem: FileSystem, sc: SparkContext, kMeansModel: KMeansModel):
  RDD[LabeledPoint] = {

    val transformed = input.map(row => {
      val (id, label, features) = transFormLine(row, kMeansModel)
      LabeledPoint.apply(label, features)
    })
    transformed
  }

  def GBT(training: RDD[LabeledPoint], test: RDD[LabeledPoint], numOfIteration: Int = 50, maxDepth: Int = 6)
  : (GradientBoostedTreesModel, Double) = {

    // Train a GradientBoostedTrees model.
    // The defaultParams for Classification use LogLoss by default.
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")

    boostingStrategy.setNumIterations(numOfIteration) // Note: Use more iterations in practice.
    boostingStrategy.getTreeStrategy.setNumClasses(2)
    boostingStrategy.getTreeStrategy.setMaxDepth(maxDepth)
    boostingStrategy.getTreeStrategy.setImpurity(org.apache.spark.mllib.tree.impurity.Gini)

    // Empty categoricalFeaturesInfo indicates all features are continuous.
    //    boostingStrategy.getTreeStrategy.setCategoricalFeaturesInfo(new java.util.HashMap[Integer, Integer]())
    //    val partitioned = training.repartition(numOfNodes)

    val model = GradientBoostedTrees.train(training, boostingStrategy)

    // Evaluate model on test instances and compute test error
    val labelAndPredictions = test.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val acc = labelAndPredictions.filter(r => r._1 == r._2).count.toDouble / test.count()
    println("ACC = " + acc)
//    println("Learned classification GBT model:\n" + model.toDebugString)
    (model, acc)
  }


  def randomForest(trainingData: RDD[LabeledPoint], testData: RDD[LabeledPoint], treeNum: Int, maxDepth: Int = 10,
                   maxBins: Int = 32): (RandomForestModel, Double) = {

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

  def writeAccTo(s: String, fileSystem: FileSystem, acc: Double) = {
    val f = fileSystem.create(new Path(s))
    f.writeUTF("acc: " + acc)
    f.close()
  }

  def main(args: Array[String]): Unit = {
    val inputFile = args(0)
    val testFile = args(1)
    val output = args(2)
    val defaultParallelism = args(3).toInt * 2
    val algo = args(4)

    val conf = new SparkConf()
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val fileSystem = org.apache.hadoop.fs.FileSystem.get(new URI(output), hadoopConf)
        conf.setMaster("local[*]")
    conf.setAppName("final_project")
    conf.set("spark.default.parallelism", defaultParallelism.toString)
    val sc = new SparkContext(conf)

    //    val cleaner = PreProcess.CleanData

    //    cleaner.getSampleFile(sc, fileSystem)

    if (fileSystem.exists(new Path(output))) fileSystem.delete(new Path(output), true)

    val labelIdx = Utils.Config.labelIdx
    val numOfPartition = defaultParallelism * 2
    println(numOfPartition)

    val geoVectors = sc.textFile(inputFile).repartition(numOfPartition)
      .map(row => {
        val cols = row.split(",")
        if (!cols(0).startsWith("SAMPLING_EVENT_ID")) {
          val kMeansTrainer = new GeoKMeans
          kMeansTrainer.getGeoVector(cols)
        } else
          Vectors.dense(Array.emptyDoubleArray)
      })
      .persist(StorageLevel.MEMORY_AND_DISK).filter(_.size > 0)

    val kMeansTrainer = new GeoKMeans
    val kMeansModel = kMeansTrainer.getModel(geoVectors, fileSystem, sc)

    geoVectors.unpersist()
    kMeansModel.save(sc, output + "/kMeansModel")

    val transformedInput = sc.textFile(inputFile).repartition(numOfPartition)
      .map(row => {
        val cols = row.split(",")

        if (!cols(0).startsWith("SAMPLING_EVENT_ID") && cols(labelIdx - 1) != "?") {
          val (id, label, features) = transFormLine(cols, kMeansModel)
          LabeledPoint(label, features)
        }
        else {
          LabeledPoint(-1, Vectors.dense(Array.emptyDoubleArray))
        }
      })
      .persist(StorageLevel.MEMORY_AND_DISK)
      .filter(_.label != -1)


//    val seed = 1234L
    val seed = 435345L
//    val splits = transformedInput.randomSplit(Array(0.7, 0.3), seed)
    val splits = transformedInput.randomSplit(Array(0.7, 0.3), seed)

    transformedInput.unpersist()
    val (trainingData, testData) = (splits(0).persist(StorageLevel.MEMORY_AND_DISK), splits(1).persist(StorageLevel.MEMORY_AND_DISK))

    println("finished pre-processing")

    val (model, acc) = randomForest(trainingData, testData, 200, maxDepth = 20)
//    val (model, acc) = GBT(trainingData, testData)

    if (algo == "RF") {
      model.save(sc, output + "/RF_model")
    } else {
      model.save(sc, output + "/GBT_model")
    }

    trainingData.unpersist()
    testData.unpersist()

    writeAccTo(output + "/validation_acc", fileSystem, acc)

    val transformedTest = sc.textFile(testFile).repartition(numOfPartition)
      .map(row => {
        val cols = row.split(",")
        if (!cols(0).startsWith("SAMPLING_EVENT_ID")) {
          val (id, label, features) = transFormLine(cols, kMeansModel)
          (id, features)
        } else {
          ("", Vectors.dense(Array.emptyDoubleArray))
        }
      })
      .filter(_._1 != "")
      .persist(StorageLevel.MEMORY_AND_DISK)

    val result = transformedTest.map(rec => {
      val predicted = model.predict(rec._2)
      rec._1 + "," + predicted
    }).repartition(1)

    result.saveAsTextFile(output + "/prediction")
  }
}

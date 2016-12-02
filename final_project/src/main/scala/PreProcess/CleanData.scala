package PreProcess

import java.util

import Models.GeoAnalysis.GeoKMeans
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{DecisionTree, GradientBoostedTrees, RandomForest}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.util.Random
//import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.{BoostingStrategy, Strategy}
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel


/**
  * Created by kingkz on 11/30/16.
  */

object CleanData {
  val sourceFile = "/Users/kple/Downloads/labeled.csv"
  val sampelFile = "randomchosen.csv"

  val skip: Set[Int] = (Seq(
    1,
    2,
//    3,
//    4,
//    5,
//    6,
//    7,
//    8,
    9,
    10,
    11,
    12,
    16,
    18,
    19
  ) ++ (1017 to 1019) ++
    (20 to 26) ++ (28 to 956) ++ (961 to 963)).toSet

  val cate: Set[Int] = ((964 to 966) ++ (1020 to 1084)).toSet
  val labelIdx = 27
  val oneToK = false

  def getSampleFile(sc: SparkContext, fs: FileSystem): Unit = {

    val random = new Random(System.nanoTime())
    val output = new Path(sampelFile)
    if (fs.exists(output)) {
      fs.delete(output, true)
    }
    val sampleRows = sc.textFile(sourceFile)
      .filter(row => {
        if (row.startsWith("SAMPLING_EVENT_ID")) false
        else random.nextDouble() < math.pow(10, -2)
      })

    sampleRows.repartition(1).saveAsTextFile(output.toString)
  }

  def generateCleanData(sc: SparkContext, data: RDD[String]): RDD[LabeledPoint] = {
    val newData = data
      .map(row => {
        if (!row.startsWith("SAMPLING_EVENT_ID")) {
          val buffer = new ListBuffer[Double]
          val withIndex = row.split(",")
          assert(withIndex.length == 1657)
          var label = 0.0
          for (i <- 0 until 1657) {
            val col = withIndex(i)
              if (skip.contains(i + 1)) buffer += 0.0
              else if (cate.contains(i + 1)) {
                var value = 0
                if (col != "?") value = col.toInt
                if (oneToK) {
                  //todo check value's range first, they might not fit as index
                  val expand: Array[Double] = new Array[Double](10)

                  if (value > -1)
                    expand(value) = 1.0

                  buffer ++= expand
                } else {
                  buffer += value
                }
              } else if (i + 1 == labelIdx) {
                if (col != "0") label = 1.0
                buffer += 0.0
              } else {
                if (col != "?")
                  buffer += col.toDouble
                else
                  buffer += 0.0
              }
            }
          LabeledPoint.apply(label, Vectors.dense(buffer.toArray).toSparse)
//          LabeledPoint.apply(label, Vectors.dense(buffer.toArray))
        } else {
          LabeledPoint.apply(0, Vectors.dense(new Array[Double](1657)).toSparse)
        }
      })
    newData
  }

  def extractAllPoints(sc: SparkContext, fileSystem: FileSystem): Unit = {
    val allPoints = sc.textFile(sampelFile)
      .map(row => {
        val cols = row.split(",")
        var label = 0
        if (cols(26) != "0" && cols(26) != "?") label = 1
        (label, cols(2).toDouble, cols(3).toDouble)
      })

    val allPointsStr = "all_points"
    if (fileSystem.exists(new Path(allPointsStr))) {
      fileSystem.delete(new Path(allPointsStr), true)
    }

    allPoints.repartition(1).saveAsTextFile("all_points")

  }


  def decisionTreeTest(cleaned: RDD[LabeledPoint], conf:SparkConf): Unit = {

    val splits = cleaned.randomSplit(Array(0.7, 0.3))
    val training = splits(0).cache()
    val test = splits(1).cache()

    val numTraining = training.count()
    val numTest = test.count()
    println(s"Training: $numTraining, test: $numTest.")
    var categoricalInfo = Map[Int, Int]()
//    for (k <- cate) {
//      categoricalInfo += (k - 1) -> 10
//    }

    val numClasses = 2
    val impurity = "gini"
//    val impurity = "entropy"
    val maxDepth = 6
    val maxBins = 32

//    val sqLContext = new SparkSession.Builder().config(conf).getOrCreate()
//    import sqLContext.implicits._
//
//    val featureIndexer = new VectorIndexer()
//      .setInputCol("features")
//      .setOutputCol("indexedFeatures")
//      .setMaxCategories(20) // features with > 4 distinct values are treated as continuous.
//      .fit(cleaned.toDF())

    val decisionTree = DecisionTree.trainClassifier(
      training,
      numClasses,
      categoricalInfo,
      impurity,
      maxDepth,
      maxBins
    )
    // Evaluate model on test instances and compute test error
    val labelAndPreds = test.map { point =>
      val prediction = decisionTree.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / test.count()
    println("Test Error = " + testErr)
    println("Learned classification tree model:\n" + decisionTree.toDebugString)
  }


  def randomForest(trainingData: RDD[LabeledPoint], testData: RDD[LabeledPoint], treeNum: Int, maxDepth:Int, maxBins: Int): Double = {

    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 5
    // Use more in practice.
    val featureSubsetStrategy = "auto"
    // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 6
    val maxBins = 28

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification forest model:\n" + model.toDebugString)
    testErr
  }

  def GBT(data: RDD[LabeledPoint]): Unit = {
    // Load and parse the data file.
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a GradientBoostedTrees model.
    // The defaultParams for Classification use LogLoss by default.
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")

    boostingStrategy.setNumIterations(10) // Note: Use more iterations in practice.
    boostingStrategy.getTreeStrategy.setNumClasses(2)
    boostingStrategy.getTreeStrategy.setMaxDepth(6)
    boostingStrategy.getTreeStrategy.setImpurity(org.apache.spark.mllib.tree.impurity.Entropy)
    // Empty categoricalFeaturesInfo indicates all features are continuous.
//    boostingStrategy.getTreeStrategy.setCategoricalFeaturesInfo(new java.util.HashMap[Integer, Integer]())

    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification GBT model:\n" + model.toDebugString)
  }

//  def randomForest(data: RDD[LabeledPoint], conf: SparkConf): Unit = {
//
//    val sparkSession =  SparkSession.builder().config(conf).getOrCreate()
////    val data = sparkSession.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")
////    val pointsTrainDf =  sparkSession.createDataFrame(training)
////    val model = new LogisticRegression()
////      .train(pointsTrainDs.as[LabeledPoint])
//
//    // Index labels, adding metadata to the label column.
//    // Fit on whole dataset to include all labels in index.
//    val schemaString = Array("label", "features")
//    val schema = schemaString.map(line => {
//      if (line == "label") {
//        StructField(line, DoubleType, nullable = true)
//      } else {
//        StructField(line, SQLDataTypes.VectorType, nullable = true)
//      }
//    })
//
//    val rows = data.map(lp => {
//      Row(lp)
//    })
//
//    val df = sparkSession.createDataFrame(rows, StructType(schema))
//
//    val labelIndexer = new StringIndexer()
//      .setInputCol("label")
//      .setOutputCol("indexedLabel")
//      .fit(df)
//    // Automatically identify categorical features, and index them.
//    // Set maxCategories so features with > 4 distinct values are treated as continuous.
//    val featureIndexer = new VectorIndexer()
//      .setInputCol("features")
//      .setOutputCol("indexedFeatures")
//      .setMaxCategories(4)
//      .fit(df)
//
//    // Split the data into training and test sets (30% held out for testing).
//    val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))
//
//    // Train a RandomForest model.
//    val rf = new RandomForestClassifier()
//      .setLabelCol("indexedLabel")
//      .setFeaturesCol("indexedFeatures")
//      .setNumTrees(10)
//
//    // Convert indexed labels back to original labels.
//    val labelConverter = new IndexToString()
//      .setInputCol("prediction")
//      .setOutputCol("predictedLabel")
//      .setLabels(labelIndexer.labels)
//
//    // Chain indexers and forest in a Pipeline.
//    val pipeline = new Pipeline()
//      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))
//
//    // Train model. This also runs the indexers.
//    val model = pipeline.fit(trainingData)
//
//    // Make predictions.
//    val predictions = model.transform(testData)
//
//    // Select example rows to display.
//    predictions.select("predictedLabel", "label", "features").show(5)
//
//    // Select (prediction, true label) and compute test error.
//    val evaluator = new MulticlassClassificationEvaluator()
//      .setLabelCol("indexedLabel")
//      .setPredictionCol("prediction")
//      .setMetricName("accuracy")
//    val accuracy = evaluator.evaluate(predictions)
//    println("Test Error = " + (1.0 - accuracy))
//
//    val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
//    println("Learned classification forest model:\n" + rfModel.toDebugString)
//  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val fileSystem = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    conf.setMaster("local[*]")
      .setAppName("final_project")

    val sc = new SparkContext(conf)
    getSampleFile(sc, fileSystem)

    val cleaned = generateCleanData(sc, sc.textFile(sampelFile))

//    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

//        val updater = params.regType match {
//            case L1 => new L1Updater()
//            case L2 => new SquaredL2Updater()
//        }
//        val updater = new L1Updater()

//        val model = params.algorithm match {
//            case LR =>
//                val algorithm = new LogisticRegressionWithLBFGS()
//                algorithm.optimizer
//                    .setNumIterations(12)
//                    .setUpdater(updater)
//                    .setRegParam(1.0)
//                val model = algorithm.run(training).clearThreshold()
//            case SVM =>
//                val algorithm = new SVMWithSGD()
//                algorithm.optimizer
//                    .setNumIterations(12)
//                    .setStepSize(0.1)
//                    .setUpdater(updater)
//                    .setRegParam(1.0)
//                val model = algorithm.run(training).clearThreshold()
//        }

//
//
//        val prediction = model.predict(test.map(_.features))
//        val predictionAndLabel = prediction.zip(test.map(_.label))
//
//        val metrics = new BinaryClassificationMetrics(predictionAndLabel)
//
//        println(s"Test areaUnderPR = ${metrics.areaUnderPR()}.")
//        println(s"Test areaUnderROC = ${metrics.areaUnderROC()}.")
//        println(s"Test precision = ${metrics.pr()}.")
//    val cleanPath = new Path("clean")
//    if (fileSystem.exists(cleanPath)) {
//      fileSystem.delete(cleanPath, true)
//    }
//    cleaned.saveAsTextFile("clean")

//    val pca = new PCA(5).fit(cleaned.map(_.features))
//
//    val projected = cleaned.map(p => p.copy(features = pca.transform(p.features)))

//    decisionTreeTest(projected)
//    decisionTreeTest(cleaned)
    GBT(cleaned)

//    val rstBuffer = new ListBuffer[String]
//    for (treeNum <- 1 to 10) {
//      for (maxDepth <- 3 to 8) {
//        for (maxBins <- 20 to 30) {
//          // Load and parse the data file.
//          // Split the data into training and test sets (30% held out for testing)
//          val splits = cleaned.randomSplit(Array(0.7, 0.3))
//          val (trainingData, testData) = (splits(0), splits(1))
//          val error = randomForest(trainingData, testData, treeNum, maxDepth, maxDepth)
//          rstBuffer += Array(cleaned, treeNum, maxDepth, maxBins, error).toString
//        }
//      }
//    }

//    val rstPath = new Path("result")
//    if (fileSystem.exists(rstPath)) fileSystem.delete(rstPath, true)
//
//    sc.makeRDD(rstBuffer).saveAsTextFile("result")

//    val points = sc.textFile("all_points")
//
//    val geo = GeoKMeans
//    geo.runKMeans(points)
    sc.stop()
  }
}
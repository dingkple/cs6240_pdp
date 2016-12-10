package PreProcess

import java.io.{BufferedWriter, File, FileWriter}
import java.util

import Models.GeoAnalysis.{GeoKMeans, MissAnalysis}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, SVMWithSGD}
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.regression.{IsotonicRegression, LabeledPoint}
import org.apache.spark.mllib.tree.{DecisionTree, GradientBoostedTrees, RandomForest}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.util.DoubleAccumulator

import scala.collection.mutable
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
//    1100,
//    1098,
//    1092,
//    1094,
//    1096,
//    1102
  ) ++ (1017 to 1019)
//    ++ (20 to 26) ++ (28 to 955)
    ++ (954 to 956) ++ (960 to 963)).toSet

  val cate: Set[Int] = ((964 to 966) ++ (1020 to 1084)).toSet
  val labelIdx = 27
  val oneToK = false

  var resultPathStr = "result4"
  val knnLoc = true


  if (!knnLoc) {
    resultPathStr = "result0"
  }

  var file = new File(resultPathStr)
  val bw = new BufferedWriter(new FileWriter(file))

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

    sampleRows.repartition(4).saveAsTextFile(output.toString)
  }

  def generateCleanData(data: RDD[String], sc: SparkContext, fileSystem: FileSystem): RDD[LabeledPoint] = {

    if (knnLoc)
      extractAllPoints(sc, fileSystem)

    val kmeanModel = KMeansModel.load(sc, Utils.Config.KMEANSOUTPUT)

//    val numAccumulator = (1 to 1657).filter(i=> !skip.contains(i) && !cate.contains(i))
//      .map(i => i -> sc.doubleAccumulator(i.toString))
//      .toMap[Int, DoubleAccumulator]

//    val cateMap: collection.mutable.Map[Int, collection.mutable.Map[Int, Int]] = collection.mutable.Map[Int, collection.mutable.Map[Int, Int]]()
//    for (k <- cate) {
//      cateMap(k) = collection.mutable.Map[Int, Int]()
//    }
//    val cateMap = cate.map(k => k -> mutable.HashMap[Int, Int]()).toMap[Int, mutable.HashMap[Int, Int]]

    val replaceMap = sc.textFile("analysis_2").map(line => {
      val kv = line.substring(1, line.length-1).split(",")
      (kv(0).toDouble, kv(1).toDouble)
    }).collectAsMap()

    val shouldReplace = false

    val newData = data
      .map(row => {
        if (!row.startsWith("SAMPLING_EVENT_ID")) {
          val buffer = new ListBuffer[Double]
          val withIndex = row.split(",")
          assert(withIndex.length == 1657)
          var label = 0.0
          for (i <- 0 until 1657) {
            val col = withIndex(i)
              if (skip.contains(i + 1)) {
                buffer += 0.0
              }
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
                  if (col != "?") {
//                    val m = cateMap(i + 1)
//                    if (m.contains(value)) {
//                      m(value) += 1
//                    } else {
//                      m(value) = 1
//                    }
                    buffer += value
                  } else {
                    if (shouldReplace) {
                      buffer += replaceMap(i)
                    } else {
                      buffer += 0.0
                    }
                  }
                }
              } else if (i + 1 == labelIdx) {
                if (col != "0" && col != "?") label = 1.0
                buffer += 0.0
              } else {
                if (col != "?") {
                  if (col == "X") {
                    buffer += 1.0
                  } else {
                    val value = col.toDouble
                    //                  numAccumulator(i+1).add(value)
                    if (knnLoc && i + 1 == 3) {
                      buffer += kmeanModel.predict(Vectors.dense(Array(withIndex(2).toDouble, withIndex(3).toDouble)))
                    } else if (knnLoc && i + 1 == 4) {
                      buffer += 0.0
                    } else {
                      buffer += value
                    }
                  }
                }
                else {
                  if (shouldReplace) {
                    buffer += replaceMap(i)
                  } else {
                    buffer += 0.0
                  }
                }
              }
            }
          LabeledPoint.apply(label, Vectors.dense(buffer.toArray).toSparse)
//          LabeledPoint.apply(label, Vectors.dense(buffer.toArray))
//          println(buffer.toString())
//          (label, buffer)
        } else {
          LabeledPoint.apply(0, Vectors.dense(new Array[Double](1657)).toSparse)
//          val buffer = new ListBuffer[Double]()
//          buffer ++ new Array[Double](1657)
//          (0.0, buffer)
        }
      })
    newData
//    newData.foreach(k => println(k._1 + k._2.toString()))
//
//    cateMap.foreach(k => println(k._2.toString()))
//
//    val maxCate = cateMap.map(k => {
//      k._1 -> k._2.values.maxBy(vv => vv)
//    })
//
//    val k = newData.count()
//
//    val allAccumulator = sc.broadcast(numAccumulator)
//
//    val replaced = newData.map(row => {
//      val buffer = row._2
//      val label: Double = row._1
//      for (i <- buffer.indices) {
//        if (buffer(i) == Int.MaxValue) {
//          val j = i+1
//          if (cate.contains(j)) {
//            buffer(j) = maxCate(j)
//          } else if (!skip.contains(j)) {
//            buffer(j) = allAccumulator.value(i + 1).value / k
//          }
//        }
//      }
//      LabeledPoint.apply(label, Vectors.dense(buffer.toArray).toSparse)
//    })
//    replaced
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
//    val points = sc.textFile("all_points")
    val points = allPoints.map(p => {
      Vectors.dense(Array(p._2, p._3))
    })
    val geo = new GeoKMeans
    geo.getModel(points, fileSystem, sc)
  }


  def decisionTreeTest(training: RDD[LabeledPoint], test: RDD[LabeledPoint], conf:SparkConf): Unit = {

//    val splits = cleaned.randomSplit(Array(0.7, 0.3))
//    val training = splits(0).cache()
//    val test = splits(1).cache()

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
    val maxDepth = 4
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
    val acc = labelAndPreds.filter(r => r._1 == r._2).count().toDouble / test.count()
    println("Acc = " + acc)

    bw.write("DecisionTree" + ' ' + maxDepth + ' ' + maxBins + ' ' + acc + '\n')
    println("Learned classification tree model:\n" + decisionTree.toDebugString)
  }


  def randomForest(trainingData: RDD[LabeledPoint], testData: RDD[LabeledPoint], treeNum: Int, maxDepth:Int = 6,
                   maxBins: Int = 32): Double = {

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

//    labelAndPreds.saveAsTextFile("clean_RF_rst")

    val testErr = labelAndPreds.filter(r => r._1 == r._2).count.toDouble / testData.count()
    println("ACC = " + testErr)
//    println("Learned classification forest model:\n" + model.toDebugString)
    testErr
  }

  var numOfNodes = 5

  def GBT(training: RDD[LabeledPoint], test: RDD[LabeledPoint]): Unit = {

    // Train a GradientBoostedTrees model.
    // The defaultParams for Classification use LogLoss by default.
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")

    val iterations = 150
    val maxDepth = 6
    boostingStrategy.setNumIterations(iterations) // Note: Use more iterations in practice.
    boostingStrategy.getTreeStrategy.setNumClasses(2)
    boostingStrategy.getTreeStrategy.setMaxDepth(maxDepth)
    boostingStrategy.getTreeStrategy.setImpurity(org.apache.spark.mllib.tree.impurity.Gini)

    // Empty categoricalFeaturesInfo indicates all features are continuous.
//    boostingStrategy.getTreeStrategy.setCategoricalFeaturesInfo(new java.util.HashMap[Integer, Integer]())
//    val partitioned = training.repartition(numOfNodes)

    val model = GradientBoostedTrees.train(training, boostingStrategy)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = test.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val acc = labelAndPreds.filter(r => r._1 == r._2).count.toDouble / test.count()
    println("ACC = " + acc)
    bw.write("GBT" + ' ' + iterations + ' ' + maxDepth + ' ' + acc + '\n')
    println("Learned classification GBT model:\n" + model.toDebugString)
  }

  def LR(training: RDD[LabeledPoint], test: RDD[LabeledPoint]): Unit = {

    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .run(training)

    // Compute raw scores on the test set.
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val accuracy = metrics.accuracy

    bw.write("LR" + ' '  + accuracy + '\n')
    println(s"Accuracy = $accuracy")
  }

  def SVM(training: RDD[LabeledPoint], test: RDD[LabeledPoint]): Unit = {

    // Run training algorithm to build the model
    val numIterations = 150
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)
  }

  def IsotonicRegression(training: RDD[LabeledPoint], test: RDD[LabeledPoint]): Unit = {
    // Create label, feature, weight tuples from input data with weight set to default value 1.0.
//    val parsedData = data.map { labeledPoint =>
//      (labeledPoint.label, labeledPoint.features(0), 1.0)
//    }

    // Split data into training (60%) and test (40%) sets.
//    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training_ = training.map { labeledPoint =>
      (labeledPoint.label, labeledPoint.features(0), 1.0)
    }
    val test_ = test.map { labeledPoint =>
      (labeledPoint.label, labeledPoint.features(0), 1.0)
    }

    // Create isotonic regression model from training data.
    // Isotonic parameter defaults to true so it is only shown for demonstration
    val model = new IsotonicRegression().setIsotonic(true).run(training_)

    // Create tuples of predicted and real labels.
    val predictionAndLabel = test_.map { point =>
      val predictedLabel = model.predict(point._2)
      (predictedLabel, point._1)
    }

    // Calculate mean squared error between predicted and real labels.
    val meanSquaredError = predictionAndLabel.map { case (p, l) => math.pow(p - l, 2) }.mean()
    println("Mean Squared Error = " + meanSquaredError)
  }

//  def randomForest(training: RDD[LabeledPoint], test: RDD[LabeledPoint, conf: SparkConf): Unit = {
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

  def analysisData(data: RDD[String], sc: SparkContext, output: String = "analysis_2"): Unit = {
    val result = data.flatMap(row => {
      val listBuffer = new ListBuffer[(Int, Seq[String])]
      val cols = row.split(",")

      for (i <- 0 until cols.length) {
        if (!skip.contains(i+1) && cols(i) != "0" && cols(i) != "X") {
          listBuffer.append((i, Seq(cols(i))))
        }
      }
      listBuffer
    }).reduceByKey(_++_)
      .map(rec =>{
        val double_ = rec._2.map(v => {
          if (v == "?") 0
          else v.toDouble
        })
        if (cate.contains(rec._1 + 1)) {
          val m = mutable.Map[Double, Int]()
          double_.foreach(r => {
            if (r != 0) {
              if (m.contains(r)) {
                m(r) += 1
              } else {
                m(r) = 1
              }
            }
          })
          (rec._1, m.keys.maxBy[Int](key => {
            m(key)
          }))
        } else {
          (rec._1, double_.sum[Double] / double_.size)
        }
      })

    result.saveAsTextFile(output)
  }

  def test(sc: SparkContext): Unit = {
    val data = Array(1,2,3,4,5,7)

    val split = sc.parallelize(data).randomSplit(Array(0.7, 0,3), 1111L)

    split.foreach(s => {
      val f = s.collect().foldLeft("")((str, b) => str + " " + b)
      println(f)
    })
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val fileSystem = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    conf.setMaster("local[*]")
      .setAppName("final_project")

    val sc = new SparkContext(conf)

//    getSampleFile(sc, fileSystem)

//    analysisData(sc.textFile(sampelFile), sc)

//    val rec = Array(1.0,1.0,1.0, 1,1, 1.1)
//
//    val m = mutable.Map[Double, Int]()
//
//    rec.foreach(r => {
//      if (m.contains(r)) {
//        m(r) += 1
//      } else {
//        m(r) = 1
//      }
//    })
//
//    m.foreach(println(_))


    val resplit = true
    val randomSplit = false
    var splits:Array[RDD[LabeledPoint]] = null

    if (resplit) {
      val data = sc.textFile(sampelFile)
      val cleaned = generateCleanData(data, sc, fileSystem)
      var seed = 1234L
      if (randomSplit) {
        seed = Random.nextLong()
      }
      splits = cleaned.randomSplit(Array(0.7, 0.3), seed)
      splits.zipWithIndex.foreach {
        case (rdd, i) => {
          val fname = "split" + i
          val fpath = new Path(fname)
          if (fileSystem.exists(fpath)) fileSystem.delete(fpath, true)
          rdd.saveAsTextFile("split" + i)
      }}
    } else {
      splits = Array(MLUtils.loadLabeledPoints(sc, "split0"), MLUtils.loadLabeledPoints(sc, "split1")).map(_.repartition(4))
    }

    val (trainingData, testData) = (splits(0), splits(1))
    println(trainingData.getNumPartitions + " " + testData.getNumPartitions)

//    cleaned.saveAsTextFile("cleaned")

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
    decisionTreeTest(trainingData, testData, conf)
//    GBT(trainingData, testData)
//    LR(trainingData, testData)

//    IsotonicRegression(trainingData, testData)
//    println(sc.defaultParallelism + " " + trainingData.)
//    randomForest(trainingData, testData, 100, maxDepth =25, maxBins = 64)

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

//    SVM(cleaned)

//    val k = MissAnalysis
//    k.checkMissing(sc, fileSystem)

    bw.close()

    sc.stop()
  }
}
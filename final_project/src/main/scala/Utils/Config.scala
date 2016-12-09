package Utils

/**
  * Created by kple on 12/2/16.
  */
object Config {
  val geo = Array(2, 3)

  val NUM_OF_FEATURE = 1657

  val TRAINDATA = "trainingData"


  val KMEANSOUTPUT = "kmeansoutput"

  val sourceFile = "/Users/kple/Downloads/labeled.csv"
  val sampelFile = "randomchosen.csv"

  val skip: Set[Int] = (Seq(
    1,
    2,
    //    3,
    //    4,
//        5,
//        6,
//        7,
//        8,
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

}

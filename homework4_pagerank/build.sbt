import sbt._
import Keys._
//import sbtassembly.AssemblyPlugin._
//import sbtassembly.AssemblyKeys._

lazy val root = (project in file(".")).
    settings(
        name := "root",
        version := "1.0",
        libraryDependencies += ("org.apache.spark" %% "spark-core" % "2.0.1"),
        libraryDependencies += "org.jsoup" % "jsoup" % "1.7.2",
        mainClass in Compile := Some("pagerank.Pagerank")
    )

//assemblySettings
//seq(assemblySettings: _*)


//// Scala Runtime
//libraryDependencies += "org.scala-lang" % "scala-library" % scalaVersion.value
//
//// Hadoop
//libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce" % "2.6.0"
//libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-common" % "2.6.0"
//libraryDependencies += "org.jsoup" % "jsoup" % "1.7.2"
//
//libraryDependencies ++= Seq(
//    "org.apache.commons" % "commons-compress" % "1.12" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
//    "org.apache.spark" % "spark-core_2.10" % "2.0.1" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
//    "org.apache.spark" % "spark-sql_2.10" % "1.6.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
//    "org.apache.hadoop" % "hadoop-common" % "2.7.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
//    "org.apache.spark" % "spark-sql_2.10" % "1.6.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
//    "org.apache.spark" % "spark-hive_2.10" % "1.6.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
//    "org.apache.spark" % "spark-yarn_2.10" % "1.6.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy")
//)
//

//
assemblyMergeStrategy in assembly := {
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
    case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
    case PathList("org", "apache", xs @ _*) => MergeStrategy.last
    case PathList("com", "google", xs @ _*) => MergeStrategy.last
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
    case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
    case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
    case "about.html" => MergeStrategy.rename
    case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
    case "META-INF/mailcap" => MergeStrategy.last
    case "META-INF/mimetypes.default" => MergeStrategy.last
//    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case "plugin.properties" => MergeStrategy.last
    case "log4j.properties" => MergeStrategy.last
    case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
}

//mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
//{
//    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//    case x => MergeStrategy.first
//}
//}


// META-INF discarding
//mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
//    {
//        case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//        case x => MergeStrategy.first
//    }
//}

//assemblyMergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
//{
//    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
//    case PathList("org", "apache", xs @ _*) => MergeStrategy.last
//    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
//    case "about.html" => MergeStrategy.rename
////    case x => old(x)
//}
//}
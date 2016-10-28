lazy val root = (project in file(".")).
    settings(
        name := "root",
        version := "1.0",
        mainClass in Compile := Some("pagerank.Pagerank")
    )


// Scala Runtime
libraryDependencies += "org.scala-lang" % "scala-library" % scalaVersion.value

// Hadoop
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce" % "2.6.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-common" % "2.6.0"
// https://mvnrepository.com/artifact/org.jsoup/jsoup
libraryDependencies += "org.jsoup" % "jsoup" % "1.7.2"

libraryDependencies ++= Seq(
    "org.apache.commons" % "commons-compress" % "1.12" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
    "org.apache.spark" % "spark-core_2.10" % "2.0.1" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
    "org.apache.spark" % "spark-sql_2.10" % "1.6.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
    "org.apache.hadoop" % "hadoop-common" % "2.7.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
    "org.apache.spark" % "spark-sql_2.10" % "1.6.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
    "org.apache.spark" % "spark-hive_2.10" % "1.6.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
    "org.apache.spark" % "spark-yarn_2.10" % "1.6.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy")

)
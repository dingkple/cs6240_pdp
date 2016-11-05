
lazy val root = (project in file(".")).
    settings(
        name := "root_spark_version2",
        version := "2.1.1",
        libraryDependencies += ("org.apache.spark" %% "spark-core" % "2.0.1"),
        mainClass in Compile := Some("pagerank.Pagerank")
    )

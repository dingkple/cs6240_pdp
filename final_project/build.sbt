//name := "final_project"
//
//version := "1.0"
//
//scalaVersion := "2.11.0"
//
//
//libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % "2.0.2",
//  "org.apache.spark" %% "spark-sql" % "2.0.2",
//  "org.apache.spark" %% "spark-mllib" % "2.0.2"
//
//)
//
//mainClass in Compile := Some("Runner.Run")



lazy val root = (project in file(".")).
  settings(
    name := "final_project",
    version := "1.0",
    scalaVersion := "2.11.0",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.0.2",
      "org.apache.spark" %% "spark-sql" % "2.0.2",
      "org.apache.spark" %% "spark-mllib" % "2.0.2"
    ),
    mainClass in Compile := Some("Runner.Run")
  )
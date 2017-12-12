lazy val root = (project in file(".")).
settings(
        name := "puma",
        version := "0.1.0-SNAPSHOT",
        scalaVersion := "2.11.6",
        crossScalaVersions := Seq("2.10.2", "2.10.3", "2.10.4", "2.10.5", "2.11.0", "2.11.1", "2.11.2", "2.11.3", "2.11.4", "2.11.5", "2.11.6")
        )

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
        "org.apache.spark" % "spark-core_2.11" % sparkVersion,
        "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
        "org.apache.spark" %% "spark-mllib" % sparkVersion
        )


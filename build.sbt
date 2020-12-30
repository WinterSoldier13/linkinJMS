
name := "linkinJMS"

version := "1.0"

scalaVersion := "2.11.11"

val sparkV = "2.4.0"

libraryDependencies += "org.apache.activemq" % "activemq-all" % "5.16.0"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" %  sparkV,
    "org.apache.spark" %% "spark-mllib" % sparkV ,
    "org.apache.spark" %% "spark-sql" % sparkV ,
    "org.apache.spark" %% "spark-hive" % sparkV ,
    "org.apache.spark" %% "spark-streaming" % sparkV ,
    "org.apache.spark" %% "spark-graphx" % sparkV
)

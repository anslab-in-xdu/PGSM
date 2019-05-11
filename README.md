# PGSM
Precisely Guided Signature Matching of Deep Packet Inspection

### Introduction

TODO

### Requirements

We run PGSM demo in following environment:

* Spark 2.2.0
* HBase 1.3.1
* Hadoop 2.7.4
* JDK 8

Source code build with following dependency:

* Scala 2.11
* org.apache.spark % spark-core % 2.2.0
* org.apache.spark % spark-sql % 2.2.0
* org.apache.spark % spark-mllib % 2.2.0
* org.apache.hadoop % hadoop-common % 2.7.4

### Installation

1. Create sbt project with following `build.sbt`


```sbt

name := "PGSM"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion  % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion  % Provided ,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided ,
  "org.apache.hadoop" % "hadoop-common" % "2.7.4"
)

enablePlugins(PackPlugin)
```

and `plugins.sbt` (in `project/plugins.sbt` directory)

```sbt
addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.11")
```
copy file `lib/usage/RandomForestModel.scala` to `src` file in the project root.

2. In command line, use `sbt pack` to compile and package the Spark program.

### Usage

Before running the program, uploading the model file (in `examples/rules/random_forest_model`) to the HDFS with command below.

```bash
hadoop fs -put random_forest_model
```

Use the command below to submit program to Spark cluster.

```bash
spark-submit --master $SPARK_MASTER_ADDRESS --class cc.xmccc.pgsm.RandomForestModel jars/pgsm_2.11-0.1.jar $MODEL_FILES $FEATURE_FILE
```

### Example

Testing with model `examples/rules/random_forest_model` and featues `examples/features/features.csv`.

![example](./examples/results/result.png)

### Feedback
issues    

Email：cosey2yhn@gmail.com

### License

GNU General Public License

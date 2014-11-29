name := "GenerateCorpus"

version := "1.0"

scalaVersion := "2.10.4"

scalacOptions += "-target:jvm-1.7"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

libraryDependencies ++=
  Seq(
    "org.apache.spark" %% "spark-core" % "1.1.0" % "provided",
    "info.bliki.wiki" % "bliki-core" % "3.0.19",
    "com.chuusai" % "shapeless_2.10.4" % "2.0.0",
    "edu.stanford.nlp" % "stanford-corenlp" % "3.4.1",
    "junit" % "junit" % "4.10" % "test"
  )

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

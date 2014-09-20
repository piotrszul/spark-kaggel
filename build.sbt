import AssemblyKeys._ // put this at the top of the file

assemblySettings

assemblyOption in assembly ~= { _.copy(includeScala = false) }

name := "spark-kaggel"

version := "1.0"

scalaVersion := "2.10.2"

// copied from scalding's build.sbt

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.0.1" % "provided"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.0.1" % "provided" 

libraryDependencies += "it.unimi.dsi" % "fastutil" % "6.5.15"

libraryDependencies += "org.apache.commons" % "commons-math3" % "3.3"

libraryDependencies += "com.github.fommil.netlib" % "all" % "1.1.2"

javacOptions ++= Seq("-source", "1.6", "-target", "1.6")

// Invocation exception if we try to run the tests in parallel
parallelExecution in Test := false


mergeStrategy in assembly <<= (mergeStrategy in assembly) {
      (old) => {
        case s if s.endsWith(".class") => MergeStrategy.last
        case s if s.endsWith("project.clj") => MergeStrategy.concat
        case s if s.endsWith(".html") => MergeStrategy.last
        case s if s.endsWith(".dtd") => MergeStrategy.last
        case s if s.endsWith(".xsd") => MergeStrategy.last
        case x => old(x)
      }
}



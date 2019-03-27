
name := "top-k-trajectories-temp"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"

addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.0-M4")
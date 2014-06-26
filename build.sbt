libraryDependencies += "commons-io" % "commons-io" % "2.4"

libraryDependencies += "com.qifun" %% "stateless-future-util" % "0.4.0"

libraryDependencies += ("org.scala-stm" %% "scala-stm" % "0.7")

scalacOptions += "-feature"

scalacOptions in Compile in doc += "-groups"

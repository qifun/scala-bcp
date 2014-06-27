libraryDependencies += "commons-io" % "commons-io" % "2.4"

libraryDependencies += "com.qifun" %% "stateless-future-util" % "0.5.0-SNAPSHOT"

libraryDependencies += ("org.scala-stm" %% "scala-stm" % "0.7")

libraryDependencies += "com.novocode" % "junit-interface" % "0.10" % "test"

scalacOptions += "-feature"

scalacOptions in Compile in doc += "-groups"

libraryDependencies += "commons-io" % "commons-io" % "2.4"

libraryDependencies += "com.qifun" %% "stateless-future-util" % "0.5.0"

libraryDependencies += ("org.scala-stm" %% "scala-stm" % "0.7")

libraryDependencies += "com.novocode" % "junit-interface" % "0.10" % "test"

scalacOptions += "-feature"

scalacOptions in Compile in doc += "-groups"

name := "scala-bcp"

organization := "com.qifun"

version := "0.1.1-SNAPSHOT"

crossScalaVersions := Seq("2.10.4", "2.11.2")

homepage := Some(url(s"https://github.com/qifun/${name.value}"))

startYear := Some(2014)

licenses := Seq("Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

publishTo <<= (isSnapshot) { isSnapshot: Boolean =>
  if (isSnapshot)
    Some("snapshots" at "https://oss.sonatype.org/content/repositories/snapshots")
  else
    Some("releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2")
}

scmInfo := Some(ScmInfo(
  url(s"https://github.com/qifun/${name.value}"),
  s"scm:git:git://github.com/qifun/${name.value}.git",
  Some(s"scm:git:git@github.com:qifun/${name.value}.git")))

pomExtra :=
  <developers>
    <developer>
      <id>Atry</id>
      <name>杨博 (Yang Bo)</name>
      <timezone>+8</timezone>
      <email>pop.atry@gmail.com</email>
    </developer>
    <developer>
      <id>chank</id>
      <name>方里权 (Fang Liquan)</name>
      <timezone>+8</timezone>
      <email>fangliquan@qq.com</email>
    </developer>
  </developers>

// vim: sts=2 sw=2 et

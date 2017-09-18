/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io._
import java.nio.file.Files

import scala.io.Source
import scala.util.Properties
import scala.collection.JavaConverters._
import scala.collection.mutable.Stack

import sbt._
import sbt.Classpaths.publishTask
import sbt.Keys._
import sbtunidoc.Plugin.UnidocKeys.unidocGenjavadocVersion
import com.simplytyped.Antlr4Plugin._
import com.sksamuel.scapegoat.sbt.ScapegoatSbtPlugin.autoImport.scapegoatVersion
import com.typesafe.sbt.pom.{PomBuild, SbtPomKeys}
import com.typesafe.tools.mima.plugin.MimaKeys
import org.scalastyle.sbt.ScalastylePlugin.autoImport._
import org.scalastyle.sbt.Tasks

import spray.revolver.RevolverPlugin._

object BuildCommons {

  val scalaBinaryVersion = "2.11"

  private val buildLocation = file(".").getAbsoluteFile.getParentFile

  val allProjects@Seq(sqlServer, tpcds) = Seq("sql-server", "tpcds").map(ProjectRef(buildLocation, _))

  // Root project.
  val spark = ProjectRef(buildLocation, "spark-sql-server")
  val sparkHome = buildLocation

  val testTempDir = s"$sparkHome/target/tmp"

  val javacJVMVersion = settingKey[String]("source and target JVM version for javac")
  val scalacJVMVersion = settingKey[String]("source and target JVM version for scalac")
}

object SparkBuild extends PomBuild {

  import BuildCommons._
  import scala.collection.mutable.Map

  val projectsMap: Map[String, Seq[Setting[_]]] = Map.empty

  override val profiles = {
    val profiles = Properties.envOrNone("SBT_MAVEN_PROFILES") match {
      case None => Seq("sbt")
      case Some(v) =>
        v.split("(\\s+|,)").filterNot(_.isEmpty).map(_.trim.replaceAll("-P", "")).toSeq
    }
    profiles
  }

  Properties.envOrNone("SBT_MAVEN_PROPERTIES") match {
    case Some(v) =>
      v.split("(\\s+|,)").filterNot(_.isEmpty).map(_.split("=")).foreach(x => System.setProperty(x(0), x(1)))
    case _ =>
  }

  override val userPropertiesMap = System.getProperties.asScala.toMap

  lazy val MavenCompile = config("m2r") extend(Compile)
  lazy val publishLocalBoth = TaskKey[Unit]("publish-local", "publish local for m2 and ivy")

  lazy val sparkGenjavadocSettings: Seq[sbt.Def.Setting[_]] = Seq(
    libraryDependencies += compilerPlugin(
      "com.typesafe.genjavadoc" %% "genjavadoc-plugin" % unidocGenjavadocVersion.value cross CrossVersion.full),
    scalacOptions ++= Seq(
      "-P:genjavadoc:out=" + (target.value / "java"),
      "-P:genjavadoc:strictVisibility=true" // hide package private types
    )
  )

  lazy val sparkScapegoatSettings: Seq[sbt.Def.Setting[_]] = Seq(
    libraryDependencies += compilerPlugin(
      "com.sksamuel.scapegoat" %% "scalac-scapegoat-plugin" % scapegoatVersion.value % Compile.name),
    scalacOptions ++= Seq(
      "-P:scapegoat:dataDir=/tmp"
    )
  )

  lazy val scalaStyleRules = Project("scalaStyleRules", file("scalastyle"))
    .settings(
      libraryDependencies += "org.scalastyle" %% "scalastyle" % "1.0.0"
    )

  lazy val scalaStyleOnCompile = taskKey[Unit]("scalaStyleOnCompile")

  lazy val scalaStyleOnTest = taskKey[Unit]("scalaStyleOnTest")

  // We special case the 'println' lint rule to only be a warning on compile, because adding
  // printlns for debugging is a common use case and is easy to remember to remove.
  val scalaStyleOnCompileConfig: String = {
    val in = "scalastyle-config.xml"
    val out = "scalastyle-on-compile.generated.xml"
    val replacements = Map(
      """customId="println" level="error"""" -> """customId="println" level="warn""""
    )
    var contents = Source.fromFile(in).getLines.mkString("\n")
    for ((k, v) <- replacements) {
      require(contents.contains(k), s"Could not rewrite '$k' in original scalastyle config.")
      contents = contents.replace(k, v)
    }
    new PrintWriter(out) {
      write(contents)
      close()
    }
    out
  }

  // Return a cached scalastyle task for a given configuration (usually Compile or Test)
  private def cachedScalaStyle(config: Configuration) = Def.task {
    val logger = streams.value.log
    // We need a different cache dir per Configuration, otherwise they collide
    val cacheDir = target.value / s"scalastyle-cache-${config.name}"
    val cachedFun = FileFunction.cached(cacheDir, FilesInfo.lastModified, FilesInfo.exists) {
      (inFiles: Set[File]) => {
        val args: Seq[String] = Seq.empty
        val scalaSourceV = Seq(file(scalaSource.in(config).value.getAbsolutePath))
        val configV = (baseDirectory in ThisBuild).value / scalaStyleOnCompileConfig
        val configUrlV = scalastyleConfigUrl.in(config).value
        val streamsV = streams.in(config).value
        val failOnErrorV = true
        val failOnWarningV = false
        val scalastyleTargetV = scalastyleTarget.in(config).value
        val configRefreshHoursV = scalastyleConfigRefreshHours.in(config).value
        val targetV = target.in(config).value
        val configCacheFileV = scalastyleConfigUrlCacheFile.in(config).value

        logger.info(s"Running scalastyle on ${name.value} in ${config.name}")
        Tasks.doScalastyle(args, configV, configUrlV, failOnErrorV, failOnWarningV, scalaSourceV,
          scalastyleTargetV, streamsV, configRefreshHoursV, targetV, configCacheFileV)

        Set.empty
      }
    }

    cachedFun(findFiles(scalaSource.in(config).value))
  }

  private def findFiles(file: File): Set[File] = if (file.isDirectory) {
    file.listFiles().toSet.flatMap(findFiles) + file
  } else {
    Set(file)
  }

  def enableScalaStyle: Seq[sbt.Def.Setting[_]] = Seq(
    scalaStyleOnCompile := cachedScalaStyle(Compile).value,
    scalaStyleOnTest := cachedScalaStyle(Test).value,
    logLevel in scalaStyleOnCompile := Level.Warn,
    logLevel in scalaStyleOnTest := Level.Warn,
    (compile in Compile) := {
      scalaStyleOnCompile.value
      (compile in Compile).value
    },
    (compile in Test) := {
      scalaStyleOnTest.value
      (compile in Test).value
    }
  )

  lazy val sharedSettings = sparkGenjavadocSettings ++ sparkScapegoatSettings ++
      (if (sys.env.contains("NOLINT_ON_COMPILE")) Nil else enableScalaStyle) ++ Seq(
    exportJars in Compile := true,
    exportJars in Test := false,
    javaHome := sys.env.get("JAVA_HOME")
      .orElse(sys.props.get("java.home").map { p => new File(p).getParentFile().getAbsolutePath() })
      .map(file),
    incOptions := incOptions.value.withNameHashing(true),
    publishMavenStyle := true,
    unidocGenjavadocVersion := "0.10",

    // Override SBT's default resolvers:
    resolvers := Seq(
      DefaultMavenRepository,
      Resolver.mavenLocal,
      Resolver.file("local", file(Path.userHome.absolutePath + "/.ivy2/local"))(Resolver.ivyStylePatterns)
    ),
    externalResolvers := resolvers.value,
    otherResolvers := SbtPomKeys.mvnLocalRepository(dotM2 => Seq(Resolver.file("dotM2", dotM2))).value,
    publishLocalConfiguration in MavenCompile :=
      new PublishConfiguration(None, "dotM2", packagedArtifacts.value, Seq(), ivyLoggingLevel.value),
    publishMavenStyle in MavenCompile := true,
    publishLocal in MavenCompile := publishTask(publishLocalConfiguration in MavenCompile, deliverLocal).value,
    publishLocalBoth := Seq(publishLocal in MavenCompile, publishLocal).dependOn.value,

    javacOptions in (Compile, doc) ++= {
      val versionParts = System.getProperty("java.version").split("[+.\\-]+", 3)
      var major = versionParts(0).toInt
      if (major == 1) major = versionParts(1).toInt
      if (major >= 8) Seq("-Xdoclint:all", "-Xdoclint:-missing") else Seq.empty
    },

    javacJVMVersion := "1.8",
    scalacJVMVersion := "1.8",

    javacOptions in Compile ++= Seq(
      "-encoding", "UTF-8",
      "-source", javacJVMVersion.value,
      "-Xlint:unchecked"
    ),
    // This -target option cannot be set in the Compile configuration scope since `javadoc` doesn't
    // play nicely with it; see https://github.com/sbt/sbt/issues/355#issuecomment-3817629 for
    // additional discussion and explanation.
    javacOptions in (Compile, compile) ++= Seq(
      "-target", javacJVMVersion.value
    ),

    scalacOptions in Compile ++= Seq(
      s"-target:jvm-${scalacJVMVersion.value}",
      "-sourcepath", (baseDirectory in ThisBuild).value.getAbsolutePath  // Required for relative source links in scaladoc
    ),

    // Implements -Xfatal-warnings, ignoring deprecation warnings.
    // Code snippet taken from https://issues.scala-lang.org/browse/SI-8410.
    compile in Compile := {
      val analysis = (compile in Compile).value
      val out = streams.value

      def logProblem(l: (=> String) => Unit, f: File, p: xsbti.Problem) = {
        l(f.toString + ":" + p.position.line.fold("")(_ + ":") + " " + p.message)
        l(p.position.lineContent)
        l("")
      }

      var failed = 0
      analysis.infos.allInfos.foreach { case (k, i) =>
        i.reportedProblems foreach { p =>
          val deprecation = p.message.contains("is deprecated")

          if (!deprecation) {
            failed = failed + 1
          }

          val printer: (=> String) => Unit = s => if (deprecation) {
            out.log.warn(s)
          } else {
            out.log.error("[warn] " + s)
          }

          logProblem(printer, k, p)

        }
      }

      if (failed > 0) {
        sys.error(s"$failed fatal warnings")
      }
      analysis
    }
  )

  def enable(settings: Seq[Setting[_]])(projectRef: ProjectRef) = {
    val existingSettings = projectsMap.getOrElse(projectRef.project, Seq[Setting[_]]())
    projectsMap += (projectRef.project -> (existingSettings ++ settings))
  }

  // Note ordering of these settings matter.
  /* Enable shared settings on all projects */
  (allProjects ++ Seq(spark))
    .foreach(enable(sharedSettings ++ DependencyOverrides.settings ++
      ExcludedDependencies.settings))

  /* Enable tests settings for all projects */
  allProjects.foreach(enable(TestSettings.settings))

  /* Catalyst ANTLR generation settings */
  enable(Catalyst.settings)(sqlServer)

  // TODO: move this to its upstream project.
  override def projectDefinitions(baseDirectory: File): Seq[Project] = {
    super.projectDefinitions(baseDirectory).map { x =>
      if (projectsMap.exists(_._1 == x.id)) x.settings(projectsMap(x.id): _*)
      else x.settings(Seq[Setting[_]](): _*)
    }
  }
}

/**
 * Overrides to work around sbt's dependency resolution being different from Maven's.
 */
object DependencyOverrides {
  lazy val settings = Seq(
    dependencyOverrides += "com.google.guava" % "guava" % "14.0.1")
}

/**
 * This excludes library dependencies in sbt, which are specified in maven but are
 * not needed by sbt build.
 */
object ExcludedDependencies {
  lazy val settings = Seq(
    libraryDependencies ~= { libs => libs.filterNot(_.name == "groovy-all") }
  )
}

object Catalyst {
  lazy val settings = antlr4Settings ++ Seq(
    antlr4Version in Antlr4 := "4.7",
    antlr4PackageName in Antlr4 := Some("org.apache.spark.sql.server.parser"),
    antlr4GenListener in Antlr4 := true,
    antlr4GenVisitor in Antlr4 := true
  )
}

object CopyDependencies {

  val copyDeps = TaskKey[Unit]("copyDeps", "Copies needed dependencies to the build directory.")
  val destPath = (crossTarget in Compile) { _ / "jars"}

  lazy val settings = Seq(
    copyDeps := {
      val dest = destPath.value
      if (!dest.isDirectory() && !dest.mkdirs()) {
        throw new IOException("Failed to create jars directory.")
      }

      (dependencyClasspath in Compile).value.map(_.data)
        .filter { jar => jar.isFile() }
        .foreach { jar =>
          val destJar = new File(dest, jar.getName())
          if (destJar.isFile()) {
            destJar.delete()
          }
          Files.copy(jar.toPath(), destJar.toPath())
        }
    },
    crossTarget in (Compile, packageBin) := destPath.value,
    packageBin in Compile := (packageBin in Compile).dependsOn(copyDeps).value
  )

}

object TestSettings {
  import BuildCommons._

  lazy val settings = Seq (
    // Fork new JVMs for tests and set Java options for those
    fork := true,
    // Setting SPARK_DIST_CLASSPATH is a simple way to make sure any child processes
    // launched by the tests have access to the correct test-time classpath.
    envVars in Test ++= Map(
      "SPARK_DIST_CLASSPATH" ->
        (fullClasspath in Test).value.files.map(_.getAbsolutePath)
          .mkString(File.pathSeparator).stripSuffix(File.pathSeparator),
      "SPARK_PREPEND_CLASSES" -> "1",
      "SPARK_SCALA_VERSION" -> scalaBinaryVersion,
      "SPARK_TESTING" -> "1",
      "JAVA_HOME" -> sys.env.get("JAVA_HOME").getOrElse(sys.props("java.home"))),
    javaOptions in Test += s"-Djava.io.tmpdir=$testTempDir",
    javaOptions in Test += "-Dspark.test.home=" + sparkHome,
    javaOptions in Test += "-Dspark.testing=1",
    javaOptions in Test += "-Dspark.port.maxRetries=100",
    javaOptions in Test += "-Dspark.master.rest.enabled=false",
    javaOptions in Test += "-Dspark.memory.debugFill=true",
    javaOptions in Test += "-Dspark.ui.enabled=false",
    javaOptions in Test += "-Dspark.ui.showConsoleProgress=false",
    javaOptions in Test += "-Dspark.unsafe.exceptionOnMemoryLeak=true",
    javaOptions in Test += "-Dsun.io.serialization.extendedDebugInfo=false",
    javaOptions in Test += "-Dderby.system.durability=test",
    javaOptions in Test ++= System.getProperties.asScala.filter(_._1.startsWith("spark"))
      .map { case (k,v) => s"-D$k=$v" }.toSeq,
    javaOptions in Test += "-ea",
    javaOptions in Test ++= "-Xmx3g -Xss4096k"
      .split(" ").toSeq,
    javaOptions += "-Xmx3g",
    // Exclude tags defined in a system property
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest,
      sys.props.get("test.exclude.tags").map { tags =>
        tags.split(",").flatMap { tag => Seq("-l", tag) }.toSeq
      }.getOrElse(Nil): _*),
    testOptions in Test += Tests.Argument(TestFrameworks.JUnit,
      sys.props.get("test.exclude.tags").map { tags =>
        Seq("--exclude-categories=" + tags)
      }.getOrElse(Nil): _*),
    // Show full stack trace and duration in test cases.
    testOptions in Test += Tests.Argument("-oDF"),
    testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),
    // Enable Junit testing.
    libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test",
    // Only allow one test at a time, even across projects, since they run in the same JVM
    parallelExecution in Test := false,
    // Make sure the test temp directory exists.
    resourceGenerators in Test += Def.macroValueI(resourceManaged in Test map { outDir: File =>
      var dir = new File(testTempDir)
      if (!dir.isDirectory()) {
        // Because File.mkdirs() can fail if multiple callers are trying to create the same
        // parent directory, this code tries to create parents one at a time, and avoids
        // failures when the directories have been created by somebody else.
        val stack = new Stack[File]()
        while (!dir.isDirectory()) {
          stack.push(dir)
          dir = dir.getParentFile()
        }

        while (stack.nonEmpty) {
          val d = stack.pop()
          require(d.mkdir() || d.isDirectory(), s"Failed to create directory $d")
        }
      }
      Seq.empty[File]
    }).value,
    concurrentRestrictions in Global += Tags.limit(Tags.Test, 1),
    // Remove certain packages from Scaladoc
    scalacOptions in (Compile, doc) := Seq(
      "-groups",
      "-skip-packages", Seq(
        "org.apache.spark.api.python",
        "org.apache.spark.network",
        "org.apache.spark.deploy",
        "org.apache.spark.util.collection"
      ).mkString(":"),
      "-doc-title", "Spark " + version.value.replaceAll("-SNAPSHOT", "") + " ScalaDoc"
    )
  )

}

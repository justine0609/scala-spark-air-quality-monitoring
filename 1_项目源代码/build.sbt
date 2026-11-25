ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "io.github.justine0609"

// 判断 sbt 运行的 JVM 主版本号（fork 后子进程沿用同一 JVM）。
// JDK 8 不认识 --add-opens（该参数自 JDK 9 引入），若无条件注入会导致
// "Unrecognized option: --add-opens" 而 JVM 直接崩溃；这里按版本决定是否附加。
lazy val jvmMajor = sys.props.get("java.version").map(_.split("\\.")(0).toInt).getOrElse(8)

lazy val root = (project in file("."))
  .settings(
    name := "aqi-project4",
    version := "1.0",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.9",
      "org.apache.spark" %% "spark-sql" % "3.5.9"
    ),
    // fork 让 Spark 在独立 JVM 中运行，避免 sbt 自身 JVM 参数与 Spark 冲突
    run / fork := true,
    run / javaOptions ++= Seq(
      "-Xmx4g",
      "-Dfile.encoding=UTF-8",
      "-Duser.timezone=Asia/Shanghai",
      // Windows 下 Spark 依赖 Hadoop 的原生库：winutils.exe 由 hadoop.home.dir 定位，
      // hadoop.dll 必须在 java.library.path 上。两者随工程分发在 hadoop/bin 下，
      // 这里用绝对路径注入，使工程换到任何一台机器都能直接 sbt run。
      s"-Dhadoop.home.dir=${baseDirectory.value.getAbsolutePath}/hadoop",
      s"-Djava.library.path=${baseDirectory.value.getAbsolutePath}/hadoop/bin"
    ) ++ (if (jvmMajor >= 9) Seq(
      // Spark 3.5 在 JDK9+ 上访问 DirectByteBuffer 需要开放这些包，否则控制台出现 illegal reflective access 告警
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
    ) else Seq.empty),
    outputStrategy := Some(StdoutOutput)
  )

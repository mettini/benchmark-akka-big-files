import scalariform.formatter.preferences._

name := """benchmark-akka-big-files"""

version := "1.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %  "akka-stream_2.11"   % "2.4.2",
  "com.typesafe.akka" %  "akka-actor_2.11"    % "2.4.2",
  "commons-pool"      % "commons-pool"        % "1.6",
  "commons-dbcp"      % "commons-dbcp"        % "1.4",
  "org.scalikejdbc"   %% "scalikejdbc"        % "2.4.2",
  "org.scalikejdbc"   %% "scalikejdbc-config" % "2.4.2",
  "mysql" % "mysql-connector-java" % "5.1.38"
)



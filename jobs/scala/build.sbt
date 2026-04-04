name := "cdc-mysql-to-mongodb-redis"
version := "1.0"
scalaVersion := "2.13.16"

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-sql"                % "4.0.0"  % "provided",
  "org.apache.spark"  %% "spark-sql-kafka-0-10"     % "4.0.0"  % "provided",
  "org.mongodb.spark" %% "mongo-spark-connector"    % "10.5.0" % "provided",
  "redis.clients"      % "jedis"                    % "5.1.0"  % "provided"
)

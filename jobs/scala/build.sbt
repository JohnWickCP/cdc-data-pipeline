name := "cdc-mysql-to-mongodb-redis"
version := "1.0"
scalaVersion := "2.12.18" // Khớp với phiên bản Scala của Spark 3.5.0

// Khai báo "provided" vì thư viện đã có sẵn trong Docker image của bạn
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0" % "provided",
  "org.mongodb.spark" %% "mongo-spark-connector" % "10.3.0" % "provided",
  "redis.clients" % "jedis" % "5.1.0" % "provided"
)
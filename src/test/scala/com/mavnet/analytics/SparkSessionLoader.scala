package com
package mavnet
package analytics

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.SparkSession

trait SparkSessionLoader {

  Logger.getRootLogger.setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  if (System.getProperty("os.name").toLowerCase.contains("win"))
    System.setProperty("hadoop.home.dir", "src/test/resources/")
  SparkSession.builder().enableHiveSupport().master("local[*]").getOrCreate()

}
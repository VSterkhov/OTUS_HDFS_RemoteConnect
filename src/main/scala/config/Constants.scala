package ru.broom
package config

import org.apache.hadoop.fs.Path

import java.util.Properties

object Constants {
  val properties = new Properties()
  properties.load(getClass.getResource("/constants.properties").openStream())

  object Hadoop {
    val STAGE_PATH: Path = new Path(properties.getProperty("hadoop.hdfs.stage.path"))
    val ODS_PATH: Path = new Path(properties.getProperty("hadoop.hdfs.ods.path"))
  }
}

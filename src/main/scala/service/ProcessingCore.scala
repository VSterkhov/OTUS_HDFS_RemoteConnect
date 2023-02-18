package ru.broom
package service

import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

import java.net.URI
import scala.util.Using

abstract class ProcessingCore extends HDFSConnect {


  Using.Manager { use =>

    val fileSystem = use(FileSystem.get(new URI("hdfs://rc1a-dataproc-m-9hemigfw309iy0ty.mdb.yandexcloud.net"), conf))

    val stagePath = new Path("/stage")
    val odsPath = new Path("/ods")



    for (status <- fileSystem.listStatus(stagePath)) {
      val s: FSDataInputStream = fileSystem.open(status.getPath)

      fileSystem.create(odsPath)



      println(status)
    }
  }

  def concate()
}

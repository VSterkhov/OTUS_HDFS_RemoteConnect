package ru.broom
package service

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.compress.CompressionCodecFactory
import java.io.{InputStream}
import scala.collection.mutable.ListBuffer
import scala.util.Using

class HDFSFileService {
  protected val conf = new Configuration()
  //conf.addResource(new Path("hdfs-site.xml"))
  //conf.addResource(new Path("core-site.xml"))
  //conf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
  //conf.set("fs.file.impl", classOf[LocalFileSystem].getName)
  System.setProperty("HADOOP_USER_NAME", "hdfs")
  System.setProperty("hadoop.home.dir", "/")

  protected val fileSystem: FileSystem = FileSystem.get(conf)

  def mkDir(distPath: Path): Boolean = {
    fileSystem.mkdirs(distPath)
  }

  def openInputStream(distPath: Path): FSDataInputStream = {
    fileSystem.open(distPath)
  }

  def getFilesWithExtension(distPath: Path, extension: String): List[Path] = {
    val buffer = new ListBuffer[Path]
    for (status <- fileSystem.listStatus(distPath)) {
      if (status.isFile) {
        val fileName = status.getPath.getName
        val distExt = fileName.substring(fileName.lastIndexOf(".")+1)
        if (distExt == extension)
          buffer.addOne(status.getPath)
      }
    }
    buffer.toList
  }
  def getDirectories(distPath: Path): List[Path] = {
    val buffer = new ListBuffer[Path]
    for (status <- fileSystem.listStatus(distPath)) {
      if (status.isDirectory)
        buffer.addOne(status.getPath)
    }
    buffer.toList
  }

  def writeContent(is: InputStream, outPath: Path): Unit = {
    Using.Manager { use =>
      val out: FSDataOutputStream = use(if (fileSystem.exists(outPath)) fileSystem.append(outPath) else fileSystem.create(outPath))
      IOUtils.copyBytes(is, out, conf)
    }
  }

  def removeFile(distPath: Path): Unit = {
    if (fileSystem.exists(distPath))
      fileSystem.delete(distPath, true)
  }

  def unGz(archPath: Path): Path = {
    val factory = new CompressionCodecFactory(conf)
    val codec = factory.getCodec(archPath)

    if (codec == null) {
      print("No codec found for " + archPath)
      return null
    }

    val outPath = new Path(CompressionCodecFactory.removeSuffix(archPath.toString, codec.getDefaultExtension))
    val is = codec.createInputStream(openInputStream(archPath))
    writeContent(is, outPath)
    outPath
  }
}

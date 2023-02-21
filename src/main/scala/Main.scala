package ru.broom

import org.apache.hadoop.fs._
import service.HDFSFileService
import config.Constants.Hadoop._

import java.io.File
import scala.util.Using

object Main {

  def main(args: Array[String]): Unit = {
    val hdfsFileService = new HDFSFileService()

    hdfsFileService.removeFile(STAGE_PATH)
    hdfsFileService.removeFile(ODS_PATH)

    val tarPath = new Path("/stage.tar.gz")

    Using(getClass.getResource(tarPath.toString).openStream()) { stream =>
      hdfsFileService.writeContent(stream, tarPath)
    }

    val tar = hdfsFileService.unGz(tarPath)

    FileUtil.unTar(hdfsFileService.openInputStream(tar), new File("/"), false)

    for (dir <- hdfsFileService.getDirectories(STAGE_PATH)){
      val stageFiles = hdfsFileService.getFilesWithExtension(dir, "csv")
      if (stageFiles.nonEmpty){
        val odsDir = new Path(ODS_PATH+"/"+dir.getName)
        val odsFile = new Path(odsDir + "/result.csv")
        hdfsFileService.mkDir(odsDir)
        stageFiles.foreach(sf=>{
          hdfsFileService.writeContent(hdfsFileService.openInputStream(sf), odsFile)
        })
      }
    }
  }
}

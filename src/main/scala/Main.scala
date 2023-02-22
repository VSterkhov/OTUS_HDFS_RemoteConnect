package ru.broom

import org.apache.hadoop.fs._
import service.HDFSFileService
import config.Constants.Hadoop._

object Main {

  def main(args: Array[String]): Unit = {
    val hdfsFileService = new HDFSFileService()

    hdfsFileService.removeFile(ODS_PATH)

    for (dir <- hdfsFileService.getDirectories(STAGE_PATH)){
      val stageFiles = hdfsFileService.getFilesWithExtension(dir, "csv")
      if (stageFiles.nonEmpty){
        val odsDir = new Path(ODS_PATH+"/"+dir.getName)
        val odsFile = new Path(odsDir + "/result.csv")
        hdfsFileService.mkDir(odsDir)
        stageFiles.foreach(sf=>{
          hdfsFileService.writeContent(hdfsFileService.openInputStream(sf), odsFile, true)
        })
      }
    }
  }
}

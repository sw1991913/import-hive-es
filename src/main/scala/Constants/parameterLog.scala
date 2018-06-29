package Constants

import Util.aboutHive

/**
 * Created by probdViewSonic on 2017/3/9.
 */
class parameterLog (confFile:String)extends Serializable{

  val hiveName=aboutHive.getValue(confFile,"spark.hive.es.import.hive.tablename").trim
  if (hiveName =="") throw new Exception(hiveName+"不能为空")

  val firstPartition=aboutHive.getValue(confFile,"spark.hive.input.groupby.firstName").trim
  if (firstPartition=="") throw new Exception(firstPartition+"不能为空")


  var outPutCols=aboutHive.getValue(confFile,"xwc.java.es.output.cols").trim
  if (outPutCols =="") outPutCols = "*"

  val timeField=aboutHive.getValue(confFile,"spark.hive.input.time.field").trim
  if (timeField=="") throw new Exception(timeField+"不能为空")
  val timeIndex=outPutCols.split(",").indexOf(timeField)

  val numpartitions=if (aboutHive.getValue(confFile,"spark.es.output.numpartitions").trim==null)  2
                     else aboutHive.getValue(confFile,"spark.es.output.numpartitions").trim.toInt

  val fileprefix=aboutHive.getValue(confFile,"xwc.java.es.fileprefix").trim
  if (fileprefix =="") throw new Exception(fileprefix+"不能为空")

  val tmpDir=aboutHive.getValue(confFile,"spark.es.output.tmpdir").trim
  if (tmpDir =="") throw new Exception("tmpDir 不能为空")

  val tolibDir=aboutHive.getValue(confFile,"spark.data.es.outputdir").trim
  if (tolibDir =="") throw new Exception(tolibDir+"不能为空")

}

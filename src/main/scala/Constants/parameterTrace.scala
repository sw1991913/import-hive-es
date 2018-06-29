package Constants

import Util.aboutHive
import org.apache.commons.lang.StringUtils

/**
  * Created by probdViewSonic on 2017/3/9.
  */
class parameterTrace(confFile:String) extends Serializable{

   val hiveName=aboutHive.getValue(confFile,"spark.hive.es.import.hive.tablename").trim
   if (hiveName =="") throw new Exception("hiveName 不能为空")

//   val database=hiveName.split('.')(0)
//   val table=hiveName.split('.')(1)

   val firstPartition=aboutHive.getValue(confFile,"spark.hive.input.groupby.firstName").trim
   if (firstPartition=="") throw new Exception("firstPartition 不能为空")

   val timeField=aboutHive.getValue(confFile,"spark.hive.input.time.field").trim
   if (timeField=="") throw new Exception("timeField 不能为空")

   val fileprefix=aboutHive.getValue(confFile,"xwc.java.es.fileprefix").trim
   if (fileprefix =="") throw new Exception("fileprefix 不能为空")

   val tmpDir=aboutHive.getValue(confFile,"spark.es.output.tmpdir").trim
   if (tmpDir =="") throw new Exception("tmpDir 不能为空")

   val tolibDir=aboutHive.getValue(confFile,"spark.es.output.tolibdir").trim
   if (tolibDir =="") throw new Exception("tolibDir 不能为空")

   val mac=aboutHive.getValue(confFile,"spark.es.hive.import.hive.mac.field").trim
   if (mac =="") throw new Exception("mac 不能为空")

   val stime=aboutHive.getValue(confFile,"spark.es.hive.import.hive.stime.field").trim
   if (stime =="") throw new Exception("stime 不能为空")

   val svc=aboutHive.getValue(confFile,"spark.es.hive.import.hive.svc.field").trim
   if (svc =="") throw new Exception("svc不能为空")

   val etime=aboutHive.getValue(confFile,"spark.es.hive.import.hive.etime.field").trim

   val numpartitions=if (aboutHive.getValue(confFile,"spark.es.output.numpartitions").trim==null)  2
               else aboutHive.getValue(confFile,"spark.es.output.numpartitions").trim.toInt

   val source=aboutHive.getValue(confFile,"spark.es.hive.import.hive.trace.source").trim
   if (StringUtils.isBlank(source)) throw new Exception("source 不能为空")
}

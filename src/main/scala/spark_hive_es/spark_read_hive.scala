package spark_hive_es

import Constants.{parameterTrace, parameterLog}
import Util.aboutHive._
import mysqlCommon.MysqlUtil
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.Map

/**
 * Created by probdViewSonic on 2017/2/22.
 */
object spark_read_hive {
    def main(args: Array[String]) {

      val optionsMap = args.map(arg => {
      if(args.length!=4){ System.exit(1) }
      System.setProperty("HADOOP_USER_NAME","root")
    //提取输入参数
        val kvParam = arg.split("=").toList
        Map(kvParam(0) -> (if (kvParam.length < 2) "" else kvParam(1)))
      }).toList.reduce((p1, p2) => p1 ++ p2)
      val conf=new Configuration()

      conf.set("fs.hdfs.impl.disable.cache","true")
      /**
       * 获取 输入参数
       * */
      val jobName=optionsMap.getOrElse("job.name", "").trim
      val starttime=optionsMap.getOrElse("starttime", "").trim
      val endtime=optionsMap.getOrElse("endtime", "").trim
      val confFile = optionsMap.getOrElse("conf.file", "").trim

      //获取 时间戳，确定时间范围
      if(StringUtils.isNotBlank(endtime)) {
        if (starttime.compareTo(endtime) >= 0 ) {
          throw new Exception("endtime要大于starttime")
        }
      }

      @transient
      val fs = FileSystem.get(conf)
      @transient
      val sparkConf = new SparkConf().setAppName(if (jobName=="") "import-hive-es" else jobName)//.setMaster("local[*]")
          sparkConf.set("spark.shuffle.consolidateFiles", "true")
      @transient
      val sc=SparkContext.getOrCreate(sparkConf)
      val sqlContext=new HiveContext(sc)
      val t1=System.currentTimeMillis()
      // 实例化 mysql 连接
      @transient
      val mysql=new MysqlUtil("paramlib")

      // 获取 confFile的 导入类型
      val importType=getValue(confFile,"spark.hive.es.import.type").trim
      if (importType =="")
      {
        throw new Exception(importType+"不能为空")
      }


         /** log 操作**/
      if(importType.equalsIgnoreCase("log")){
         println("start Import-hive-es log")
         val Constant=new parameterLog(confFile)
         val df=sqlContext.sql("select "+ Constant.outPutCols+ " from "+Constant.hiveName+" where "+Constant.firstPartition+" = "+timeStamp2Day(starttime)+
           " and "+Constant.timeField+" >= "+starttime+" and "+Constant.timeField+" < " +endtime )
        /**   df 为空 ---> 直接 return 更改时间  **/
        val RDD=df.rdd
        if(RDD.isEmpty()){
          // 往mysql写 值
          mysql.overWriteOrAppend(jobName,"stopTime",endtime)
          mysql.overWriteOrAppend(jobName,"resultNums","0")
          System.exit(0)
        }
        try {
           //过滤 时间为 10位数字
        val rowRDD=RDD.map(x=>(x.getLong(Constant.timeIndex).toString.replaceAll("\"",""),x.mkString("\t").replaceAll("\"","")))
          .filter(x=>isNumber(x._1)).coalesce(Constant.numpartitions,false)
        val isSuccess= HiveWriteHDFS(rowRDD,Constant.tolibDir,starttime,mysql:MysqlUtil,jobName,sc,Constant.fileprefix,endtime)

        }catch {
          case e:Throwable => println(e.getMessage)
        }

      } /** trace 操作  **/
      else if (importType.equalsIgnoreCase("trace")){
        println("start Import-hive-es trace")
        val Constant=new parameterTrace(confFile)
         println("hiveName ==> "+Constant.hiveName)
        val RDD=  if (Constant.source.equalsIgnoreCase("2")){  // 审计 termiplog
          val df=sqlContext.sql("select "+Constant.mac+" , "+Constant.stime+" , "+Constant.etime+" , "+Constant.svc+" from "
            +Constant.hiveName+" where  "+ Constant.firstPartition+" = "+ timeStamp2Day(starttime)
            +" and "+Constant.timeField+" >= "+starttime+
            " and "+Constant.timeField+" <" + endtime )
        //  df.cache()
          /**   df 为空 ---> 直接 return 更改时间  **/
          val df1= df.drop(Constant.etime).rdd
          val df2= df.drop(Constant.stime).filter("ISNULL("+Constant.etime+")=false").rdd
          df1.union(df2)  // 合并 RDD

        }else {  // 嗅探 maclog
          val df=sqlContext.sql("select "+Constant.mac+" , "+Constant.stime+" , "+Constant.svc+" from "
           +Constant.hiveName+" where  "+ Constant.firstPartition+" = "+ timeStamp2Day(starttime)
           +" and "+Constant.timeField+" >= "+starttime+
           " and "+Constant.timeField+" <" + endtime )
           df.rdd
        }
        /**   df 为空 ---> 直接 return 更改时间  **/
        if(RDD.isEmpty()){
          // 往mysql写 值
          mysql.overWriteOrAppend(jobName,"stopTime",endtime)
          mysql.overWriteOrAppend(jobName,"resultNums","0")
          System.exit(0)
        }
        try {  // 过滤time 为10位数字,mac 要考虑到手机号, svc 是14位长度
        val rowRDD=RDD.map(x=>(x.getString(0).replaceAll("\"",""),(x.getLong(1),x.getString(2).replaceAll("\"",""))))
           .filter(x=>(x._1.length>10 && StringUtils.isNotBlank(x._2._1.toString)
          && x._2._2.length==14 && isNumber(x._2._1.toString)))
        println("source  => "+Constant.source+",day  =>  "+timeStamp2Day(starttime))

        val resultRDD=handlerTrace(rowRDD).map {
          x=> (x._2.toString,x._1+"\t"+x._2+"\t"+x._3+"\t"+x._4+"\t"+Constant.source)}.coalesce(Constant.numpartitions,false)

        val isSuccess=HiveWriteHDFS(resultRDD,Constant.tolibDir,starttime,mysql:MysqlUtil,jobName,sc,Constant.fileprefix,endtime)

        }catch {
          case e:Throwable => println(e.getMessage)
        }

      }else {
        throw new Exception("importType应该设置为log或者trace")
      }
      val t6=System.currentTimeMillis()
      println("耗时 == "+(t6-t1)/1000f)

      fs.close()
      sqlContext.clearCache()
      sc.stop()
      System.exit(0)
    }
   }

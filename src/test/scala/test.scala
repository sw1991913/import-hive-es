import MyLog.MyDfsLog
import Util.aboutHive
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by probdViewSonic on 2017/6/6.
  */
object test {
   def main(args:Array[String]): Unit ={
      val list=new ListBuffer[String]
     println(list.size+","+list.nonEmpty)


   }
 }

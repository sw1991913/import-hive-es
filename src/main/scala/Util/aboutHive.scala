package Util

import java.net.URL
import java.text.SimpleDateFormat
import java.util.regex.Pattern
import mysqlCommon.MysqlUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.{AccumulatorParam, SparkContext}
import org.apache.spark.rdd.RDD
import org.dom4j.Element
import org.dom4j.io.{XMLWriter, OutputFormat, SAXReader}
import scala.collection.mutable.ListBuffer
import scala.util.Random
import java.io.File
import scala.util.control.Breaks

/**
 * Created by probdViewSonic on 2017/2/27.
 */
object aboutHive {

  // 生成随机数0-n
  def random(x:Int):Int={
    val r=new Random()
    r.nextInt(x)
  }

  //判断是数字
  // 判断是否能转成 10位数字
  def  isNumber(str:String) = {
    Pattern.compile("[0-9]{10}").matcher(str).matches()
  }

  // 生成Name路径名
  def getName(@transient fs:FileSystem,baseDir:String,day:String,prefix:String):String={
    // 命名为 /maclog_tmp/maclog_if_day_timestamp_no.sld
    val num=baseDir.split(",").length
    val str= if (num==1){ baseDir}else{
      val r=random(num)
      baseDir.split(",")(r)
    }
    val timeStamp=System.currentTimeMillis
    val basePath=str+"/"+prefix+day+"_"+timeStamp+"_"
    basePath+random(1000)+".sld"
  }

  // 生成Name路径名2
  def getName2(@transient fs:FileSystem,baseDir:String,day:String,prefix:String):String={
    // 命名为 /maclog_tmp/mactrace_if_day_timestamp_no.sld
     var path=""
     do{
      val timeStamp=System.currentTimeMillis
      val basePath=baseDir+"/"+prefix+day+"_"+timeStamp+"_"
      path = basePath+random(10000000)+".sld"
     } while(fs.exists(new Path(path)))
    path
  }
// 清空临时目录
  def deleteTmpDir(tmpPath:Path,@transient fs:FileSystem):Unit = {
    if (!fs.exists(tmpPath)){
       fs.mkdirs(tmpPath)
       return
    }

    val filest = fs.getFileStatus(tmpPath)
    // 为文件
    if (filest.isFile) {
      fs.delete(tmpPath,false)
      fs.mkdirs(tmpPath)
      return
    }
    // 只能是 目录
    val fileArray = fs.listStatus(tmpPath)
    if (fileArray.isEmpty) return

    for (subFile <- fileArray) {
      val path=subFile.getPath
        fs.delete(path,true)
    }
  }

  // reName
  def tmpName2tolibName(tmpPath:String,tmpBaseDir:String,tolibdir:String): Path ={
    //   用/maclog/probd01 来替换 /maclog_tmp
    val num=tolibdir.split(",").length
    var str=""
    if (num==1){
      str=tmpPath.replaceFirst(tmpBaseDir,tolibdir)
    }else{
      val r=random(num)
      str=tmpPath.replaceFirst(tmpBaseDir,tolibdir.split(",")(r))
    }
      println("rename:"+tmpPath+"-->"+str)
     new Path(str)
  }

  // 数据分组 组装
  //分组写操作
  def HiveWriteHDFS2(fileRDD: RDD[(String, String)], tmpdir: String, starttime: String, @transient mysql:MysqlUtil,
                    jobName: String, @transient sc: SparkContext, prefix: String,stoptime:String): Boolean = {
    var resultNums = sc.accumulator(0L)
    var endTime = sc.accumulator(starttime.toLong)(TimeAcculumatorParam)

    val maxStime:Long=fileRDD.map(_._1.toLong).max()

    fileRDD.foreachPartition {
      x => {
        if(x.nonEmpty) {

          val configuration = new Configuration()
          val hdfs = FileSystem.get(configuration)
          val pathName = aboutHive.getName2(hdfs, tmpdir, timeStamp2Day(starttime), prefix)
          val filePath = new Path(pathName)
          println("path ==>"+pathName)
          val dos: FSDataOutputStream = hdfs.create(filePath)
          x.foreach {
            y => {
              try {
                dos.write((y._2 + "\n").getBytes())
                resultNums += 1L
                endTime += y._1.toLong
              }
              catch {
                case e:Throwable =>System.err.println(e.getMessage)
              }
            }
          }
          dos.close()
        }
      }
    }
    //判断取值
    if(fileRDD.count()==resultNums.value || maxStime== endTime.value){
      mysql.overWriteOrAppend(jobName,"stopTime",stoptime)
      mysql.overWriteOrAppend(jobName,"resultNums",resultNums.toString)
    }
    else{
      mysql.overWriteOrAppend(jobName,"stopTime",(endTime.value+(if (endTime.value < stoptime.toLong) 1 else 0)).toString)
      mysql.overWriteOrAppend(jobName,"resultNums",resultNums.toString)
    }
    true
  }

  // 遍历 目录，更名
  //递归文件目录，删除 libPath空目录
  def reNameFile(tmpDir:String,toLibDir:String,@transient fs:FileSystem):Unit = {
    val tmpPath= new Path(tmpDir)
    val filest = fs.getFileStatus(tmpPath)
    // 为文件
    if (filest.isFile) {
      fs.delete(tmpPath,false)
      fs.mkdirs(tmpPath)
      return
     }
    // 只能是 目录
      val fileArray = fs.listStatus(tmpPath)
      if (fileArray.isEmpty) return

        for (subFile <- fileArray) {
          val path=subFile.getPath
          if (subFile.isFile){
            val destDir=tmpName2tolibName(path.toString,tmpDir,toLibDir)
            if (!fs.exists(destDir)) {
              fs.mkdirs(destDir.getParent)
            }
            fs.rename(path,destDir)
          }else{
            fs.delete(path,true)
          }
        }
  }


  // 数据分组 组装
  //分组写操作
  def HiveWriteHDFS(fileRDD: RDD[(String, String)], tolibdir: String, starttime: String, @transient mysql:MysqlUtil,
                    jobName: String, @transient sc: SparkContext, prefix: String,stoptime:String): Boolean = {
    var resultNums = sc.accumulator(0L)
    var endTime = sc.accumulator(starttime.toLong)(TimeAcculumatorParam)

    val maxStime:Long=fileRDD.map(_._1.toLong).max()

    fileRDD.foreachPartition {
      x => {
        if(x.nonEmpty) {
          val configuration = new Configuration()
          val hdfs = FileSystem.get(configuration)
          val pathName = aboutHive.getName(hdfs, tolibdir, aboutHive.timeStamp2Day(starttime), prefix)
          val filePath = new Path(pathName)

          val dos: FSDataOutputStream = hdfs.create(filePath)
          x.foreach {
            y => {
              try {
                dos.write((y._2 + "\n").getBytes())
                resultNums += 1L
                endTime += y._1.toLong
              }
              catch {
                case e:Throwable =>System.err.println(e.getMessage)
              }
            }
          }
          dos.close()
        }
      }
    }
    //判断取值
    if(fileRDD.count()==resultNums.value || maxStime== endTime.value){
      mysql.overWriteOrAppend(jobName,"stopTime",stoptime)
      mysql.overWriteOrAppend(jobName,"resultNums",resultNums.toString)
    }
    else{
      mysql.overWriteOrAppend(jobName,"stopTime",(endTime.value+(if (endTime.value < stoptime.toLong) 1 else 0)).toString)
      mysql.overWriteOrAppend(jobName,"resultNums",resultNums.toString)
    }
    true
  }


  // trace 轨迹处理
  def handlerTrace(inputTableRDD:RDD[(String,(Long,String))])={
    val rddGroupBymac = inputTableRDD.groupByKey().mapValues(x=>x.toList.sortBy(y=>y._1))
    var v=new scala.collection.mutable.ListBuffer[Tuple3[Long,String,Long]]
    var Mac:String=null
    //-------------------------逻辑处理1--------------------------------------
    //-----------------------------------------------------------------------
    val s=rddGroupBymac.mapPartitions { x => x.map {
      y => {
        ////误区：clear很重要，注意：这里要清空 集合 ，让每个分组的数据 得到的数据没有重复
        v.clear()
        // 获取 每个mac分组的 起始stime svc
        Mac = y._1
        val list = y._2
        var stime: Long = list(0)._1
        var svc: String = list(0)._2
        var etime: Long = stime
        val size = list.size
        // 处理前 size-1 个元素
        if (size == 1) {
          v += Tuple3(stime, svc, etime)
        }
        else {
          for (i <- 1 until size) {
            // 断点开始，重新赋值，break到 下一轮
            if (list(i)._2.equalsIgnoreCase(svc)) {
              // 判断时间 <2小时  单位是 s
              if (list(i)._1 - etime < 7200) {
                etime = list(i)._1
              }else{
                //就用它作为离开时间，直接添加断点，加到v里面
                v += Tuple3(stime, svc, etime)
                stime = list(i)._1
                svc = list(i)._2
                etime = stime
              }
            }
            // 判断 不是同一个 svc , 直接添加断点,并重新赋值 stime svc etime
            else {
              v += Tuple3(stime, svc, etime)
              stime = list(i)._1
              svc = list(i)._2
              etime = stime
            }
          }
          //把第size个数据 加到 v里 stime svc etime
          v += Tuple3(stime, svc, etime)
        }
        (Mac,v.toList)
      }
    }
    }

    //---------------------逻辑处理2  对步骤1 2的一个 进一步处理----------------
    //-----------------------------------------------------------------------
    // 形成stime~~etime的rdd     (String,Interable(Long,String,Long))
    // val createdRDD=s.groupByKey().mapValues(x=>x.toList.sortBy(y=> y._1))
    // 对模糊区域 ，判断依据就是 下一条元素的stime减去本次的etime 要小于 300秒
    // 且下一条的etime减去stime要小于300秒
    //定义一个 容器 来承载 模糊区数据 ,对符合要求的元素 添加到v里面去，并做下一步处理
    // createdRDD已经是按照stime排过序的
    // 如果是满足要求，那就进入自定义集合v 处理，否则直接就put 到表里面去
    //定义 count1  count2 进行比较，当开始之后，都归0，知道不相等之后，可以认为是没再增加了，这时候可以去处理v
    var count1=0L
    var count2=0L
    //此处的作用是：将合适的数据归并为 一个RDD
    val resultRDD= s.mapPartitions{
      x=>{
        x.map{
          //定义一个集合List(MAC,stime,svc,etime)
          var listbuffer=new scala.collection.mutable.ListBuffer[Tuple2[String,Tuple3[Long,String,Long]]]
          var listTotal=new scala.collection.mutable.HashSet[(String,Long,String,Long)]
          //定义 下一组元素（stime ，svc, etime）
          var tuple:(Long,String,Long)=null
          var current_stime=0L
          var current_svc:String=null
          var current_etime=0L
          var old_stime = 0L
          var old_svc:String = null
          var old_etime = 0L
          y=> {
            listTotal.clear()
            var flag = false
            val MAC = y._1
            val list = y._2
            val length = list.size
            //获取第一组元素，并将第一个元素 put
            tuple = list(0)
            old_stime = tuple._1
            old_svc = tuple._2
            old_etime = tuple._3
            //1 total添加 第一个数据
            listTotal.+=((MAC,old_stime,old_svc,old_etime))
            if (length == 0) throw new Exception("聚合后的RDD 出错")
            else if (length > 1)  {
              //遍历元素，寻找 符合条件的元素到 v中进行处理  或者 直接put到表里面去
              for (i <- 1 until length) {
                current_stime = list(i)._1
                current_svc = list(i)._2
                current_etime = list(i)._3
                //进行条件判断,添加元素到 v ,否则不满足条件的 形成断点
                if ((current_etime - current_stime) < 300 && (current_stime - old_etime) < 1800) {
                  listbuffer += Tuple2(MAC, (current_stime, current_svc, current_etime))
                  flag = true
                  count2 += 1L
                  //开始 归0
                } else {
                  //就将本条数据进行作为 断点处理
                 // val rowkey = MAC + '|' + current_stime + '|' + current_svc
                  //2 total添加数据
                  listTotal.+=((MAC,current_stime,current_svc,current_etime))
                }
                // 把这次值重新赋值，以便于下次计算
                old_stime = current_stime
                old_svc = current_svc
                old_etime = current_etime
                //如果开始了，那就归0 计算  count1会比 count2 多1
                if (flag) count1 += 1L
                //按照flag开启后  count2是否有再增加 没有增加则可以进行处理v
                if (listbuffer.size>0) {
                  if ((count1 != count2) || i == (length-1)) {
                    //对v进行处理
                    val aaa=handlerList(listbuffer)
                    //v 归0   计数器count1与count2 也要归0
                    count2 = 0L
                    count1 = count2
                    listbuffer.clear()
                    flag = false
                    //3 total合并上 模糊区域的 集合v2
                    listTotal ++= aaa
                  }
                }
              }
            }
            listTotal.toList
          }
        }
      }
    }
     resultRDD.flatMap(x=>x)
  }


  // 处理trace2
  def handlerTrace2(inputTableRDD:RDD[(String,(Long,String))])={
    val rddGroupBymac = inputTableRDD.groupByKey().mapValues(x=>x.toList.sortBy(y=>y._1))
    var v=new scala.collection.mutable.ListBuffer[Tuple3[Long,String,Long]]
    var Mac:String=null
    //-------------------------逻辑处理1--------------------------------------
    //-----------------------------------------------------------------------
    val s=rddGroupBymac.mapPartitions { x => x.map {
      y => {
        ////误区：clear很重要，注意：这里要清空 集合 ，让每个分组的数据 得到的数据没有重复
        v.clear()
        // 获取 每个mac分组的 起始stime svc
        Mac = y._1
        val list = y._2
        var stime: Long = list(0)._1
        var svc: String = list(0)._2
        var etime: Long = stime
        val size = list.size
        // 处理前 size-1 个元素
        if (size == 1) {
          v += Tuple3(stime, svc, etime)
        }
        else {
          for (i <- 1 until size) {
            // 断点开始，重新赋值，break到 下一轮
            if (list(i)._2.equalsIgnoreCase(svc)) {
              // 判断时间 <2小时  单位是 s
              if (list(i)._1 - etime < 7200) {
                etime = list(i)._1
              }else{
                //就用它作为离开时间，直接添加断点，加到v里面
                v += Tuple3(stime, svc, etime)
                stime = list(i)._1
                svc = list(i)._2
                etime = stime
              }
            }
            // 判断 不是同一个 svc , 直接添加断点,并重新赋值 stime svc etime
            else {
              v += Tuple3(stime, svc, etime)
              stime = list(i)._1
              svc = list(i)._2
              etime = stime
            }
          }
          //把第size个数据 加到 v里 stime svc etime
          v += Tuple3(stime, svc, etime)
        }
        (Mac,v.toList)
      }
    }
    }

    //---------------------逻辑处理2  对步骤1 2的一个 进一步处理----------------
    //-----------------------------------------------------------------------
    // 形成stime~~etime的rdd     (String,Interable(Long,String,Long))
    // val createdRDD=s.groupByKey().mapValues(x=>x.toList.sortBy(y=> y._1))
    // 对模糊区域 ，判断依据就是 下一条元素的stime减去本次的etime 要小于 300秒
    // 且下一条的etime减去stime要小于300秒
    //定义一个 容器 来承载 模糊区数据 ,对符合要求的元素 添加到v里面去，并做下一步处理
    // createdRDD已经是按照stime排过序的
    // 如果是满足要求，那就进入自定义集合v 处理，否则直接就put 到表里面去
    //定义 count1  count2 进行比较，当开始之后，都归0，知道不相等之后，可以认为是没再增加了，这时候可以去处理v
    //此处的作用是：将合适的数据归并为 一个RDD
    val resultRDD= s.mapPartitions{
      x=>{
        x.map{
          //定义一个集合List(MAC,stime,svc,etime)
          var listbuffer=new scala.collection.mutable.ListBuffer[Tuple2[String,Tuple3[Long,String,Long]]]
          var listTotal=new scala.collection.mutable.HashSet[(String,Long,String,Long)]
          //定义 下一组元素（stime ，svc, etime）
          var tuple:(Long,String,Long)=null
          var current_stime=0L
          var current_svc:String=null
          var current_etime=0L
          var old_stime = 0L
          var old_svc:String = null
          var old_etime = 0L
          y=> {
            listbuffer.clear()
            listTotal.clear()
            val MAC = y._1
            val list = y._2
            if (list.nonEmpty) {
              //if (list.isEmpty)
              val length = list.size
              //获取第一组元素，并将第一个元素 put
              tuple = list(0)
              old_stime = tuple._1
              old_svc = tuple._2
              old_etime = tuple._3
              listbuffer += Tuple2(MAC, (old_stime, old_svc, old_etime))
              //遍历元素，寻找 符合条件的元素到 v中进行处理  或者 直接put到表里面去
              if (length>1) {
                for (i <- 1 until length) {
                  current_stime = list(i)._1
                  current_svc = list(i)._2
                  current_etime = list(i)._3
                  //进行条件判断,添加元素到 v ,否则不满足条件的 形成断点
                  if ((current_etime - current_stime) < 300 && (current_stime - old_etime) < 300 ) {
                    listbuffer += Tuple2(MAC, (current_stime, current_svc, current_etime))
                  } else {
                    //就认为 断点已经结束
                    //处理掉之前的数据 listBuffer，total添加数据
                    val aaa = handlerList(listbuffer)
                    listTotal ++= aaa
                    listbuffer.clear()
                    listbuffer += Tuple2(MAC, (current_stime, current_svc, current_etime))
                  }
                  old_stime = current_stime
                  old_svc = current_svc
                  old_etime = current_etime
                }
              }
              listTotal ++= handlerList(listbuffer)
            }
            listTotal.toList
          }
        }
      }
    }
    resultRDD.flatMap(x=>x)
  }


  //时间戳转换成 天数
  def timeStamp2Day(timeStamp:String):String={
    val format =new SimpleDateFormat("yyyyMMdd")
    val time= {
    if (timeStamp.replaceAll(",","").length==10)
      (timeStamp+"000").toLong
     else
       timeStamp.toLong
    }
      format.format(time)
  }
  //解析 dataCols 得到下标
  def getIndex(reg:String,str:String):Int={
    val arr=str.split(",")
    var i= -1
    for (i <- 0 until arr.length) {
      if (arr(i) == reg) {
        return i
      }
    }
     i
  }

  // 4.  获取XML中 conf对应值
  def getValue(confFile:String,name:String):String={
    var xmlName:Element=null
    var xmlVal:Element=null

    val saxReader=new SAXReader()
    val file=new File(confFile)   //***
    val docConf=saxReader.read(file)
    val xmlRoot:Element=docConf.getRootElement
    val xmlPropertyList=xmlRoot.elementIterator("property")

    while (xmlPropertyList.hasNext) {
      val em: Element = xmlPropertyList.next()
      val xmlName = em.element("name")
      if (xmlName.getText.equalsIgnoreCase(name)) {
        xmlVal = em.element("value")
        if (xmlVal == null) {
          xmlVal = em.addElement("value")
        }
        return xmlVal.getText
      }
    }
    null
  }

  // 3.  设置setConfMark  （单个 设置）
  def setConfMark(@transient fs:FileSystem,confFile:String,n:String,v1:String,v2:String) = {
    var xmlName:Element=null
    var endtime:Element=null
    var resultnums:Element=null
    val loop = new Breaks

    val saxReader=new SAXReader()
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())
    val url=new URL(confFile)
    val in=url.openStream()
    val docConf=saxReader.read(in)
    val xmlRoot=docConf.getRootElement
    val xmlPropertyList=xmlRoot.elementIterator("property")
    var flag:Boolean=false

    loop.breakable {
      while (xmlPropertyList.hasNext) {
        val em: Element = xmlPropertyList.next()
        val xmlName = em.element("jobName")
        if (xmlName!=null) {
          if (xmlName.getText.equalsIgnoreCase(n)) {
            endtime = em.element("stopTime")
            if (endtime == null) {
              endtime = em.addElement("stopTime")
            }
            endtime.setText(v1)

            resultnums = em.element("resultNums")
            if (resultnums == null) {
              resultnums = em.addElement("resultNums")
            }
            resultnums.setText(v2)
            flag = true
            loop.break()
          }
        }
      }
    }
    // 如果没找到，就创建节点
    if (!flag) {
      val e1=xmlRoot.addElement("property")
      e1.addElement("jobName").addText(n)
      e1.addElement("stopTime").addText(v1)
      e1.addElement("resultNums").addText(v2)
    }
    val out=fs.create(new Path(confFile))
    val format=OutputFormat.createPrettyPrint()
    val xmlWriter=new XMLWriter(out,format)
    xmlWriter.write(docConf)
    xmlWriter.close()
  }

  // termlog -> mactrace
  // trace 轨迹处理
  def termlogTrace(inputTableRDD:RDD[(String,(Long,String,Long))])={
    val rddGroupBymac = inputTableRDD.groupByKey().mapValues(x=>x.toList.sortBy(y=>y._1))
    var v=new scala.collection.mutable.ListBuffer[Tuple3[Long,String,Long]]
    var Mac:String=null
    //-------------------------逻辑处理1--------------------------------------
    //-----------------------------------------------------------------------
    val s=rddGroupBymac.mapPartitions { x => x.map {
      y => {
        ////误区：clear很重要，注意：这里要清空 集合 ，让每个分组的数据 得到的数据没有重复
        v.clear()
        // 获取 每个mac分组的 起始stime svc
        Mac = y._1
        val list = y._2
        var stime: Long = list(0)._1
        var svc: String = list(0)._2
        var etime: Long = list(0)._3
        val size = list.size
        // 处理前 size-1 个元素
        if (size == 1) {
          v += Tuple3(stime, svc, etime)
        }
        else {
          for (i <- 1 until size) {
            // 断点开始，重新赋值，break到 下一轮
            if (list(i)._2.equalsIgnoreCase(svc)) {
              // 判断时间 <2小时  单位是 s
              if (list(i)._1 - etime < 7200) {
                etime = list(i)._3
              }else{
                //就用它作为离开时间，直接添加断点，加到v里面
                v += Tuple3(stime, svc, etime)
                stime = list(i)._1
                svc = list(i)._2
                etime = list(i)._3
              }
            }
            // 判断 不是同一个 svc , 直接添加断点,并重新赋值 stime svc etime
            else {
              v += Tuple3(stime, svc, etime)
              stime = list(i)._1
              svc = list(i)._2
              etime = list(i)._3
            }
          }
          //把第size个数据 加到 v里 stime svc etime
          v += Tuple3(stime, svc, etime)
        }
        (Mac,v.toList)
      }
    }
    }

    //---------------------逻辑处理2  对步骤1 2的一个 进一步处理----------------
    //-----------------------------------------------------------------------
    // 形成stime~~etime的rdd     (String,Interable(Long,String,Long))
    // val createdRDD=s.groupByKey().mapValues(x=>x.toList.sortBy(y=> y._1))
    // 对模糊区域 ，判断依据就是 下一条元素的stime减去本次的etime 要小于 300秒
    // 且下一条的etime减去stime要小于300秒
    //定义一个 容器 来承载 模糊区数据 ,对符合要求的元素 添加到v里面去，并做下一步处理
    // createdRDD已经是按照stime排过序的
    // 如果是满足要求，那就进入自定义集合v 处理，否则直接就put 到表里面去
    //定义 count1  count2 进行比较，当开始之后，都归0，知道不相等之后，可以认为是没再增加了，这时候可以去处理v
    var count1=0L
    var count2=0L
    //此处的作用是：将合适的数据归并为 一个RDD
    val resultRDD= s.mapPartitions{
      x=>{
        x.map{
          //定义一个集合List(MAC,stime,svc,etime)
          var listbuffer=new scala.collection.mutable.ListBuffer[Tuple2[String,Tuple3[Long,String,Long]]]
          var listTotal=new scala.collection.mutable.HashSet[(String,Long,String,Long)]
          //定义 下一组元素（stime ，svc, etime）
          var tuple:(Long,String,Long)=null
          var current_stime=0L
          var current_svc:String=null
          var current_etime=0L
          var old_stime = 0L
          var old_svc:String = null
          var old_etime = 0L
          y=> {
            listTotal.clear()
            var flag = false
            val MAC = y._1
            val list = y._2
            val length = list.size
            //获取第一组元素，并将第一个元素 put
            tuple = list(0)
            old_stime = tuple._1
            old_svc = tuple._2
            old_etime = tuple._3
            //1 total添加 第一个数据
            listTotal.+=((MAC,old_stime,old_svc,old_etime))
            if (length == 0) throw new Exception("聚合后的RDD 出错")
            else if (length > 1)  {
              //遍历元素，寻找 符合条件的元素到 v中进行处理  或者 直接put到表里面去
              for (i <- 1 until length) {
                current_stime = list(i)._1
                current_svc = list(i)._2
                current_etime = list(i)._3
                //进行条件判断,添加元素到 v ,否则不满足条件的 形成断点
                if ((current_etime - current_stime) < 300 && (current_stime - old_etime) < 300) {
                  listbuffer += Tuple2(MAC, (current_stime, current_svc, current_etime))
                  flag = true
                  count2 += 1L
                  //开始 归0
                } else {
                  //就将本条数据进行作为 断点处理
                  //2 total添加数据
                  listTotal.+=((MAC,current_stime,current_svc,current_etime))
                }
                // 把这次值重新赋值，以便于下次计算
                old_stime = current_stime
                old_svc = current_svc
                old_etime = current_etime
                //如果开始了，那就归0 计算  count1会比 count2 多1
                if (flag) count1 += 1L
                //按照flag开启后  count2是否有再增加 没有增加则可以进行处理v
                if (listbuffer.size>0) {
                  if ((count1 != count2) || i == (length-1)) {
                    //对v进行处理
                    val aaa=handlerList(listbuffer)
                    //v 归0   计数器count1与count2 也要归0
                    count2 = 0L
                    count1 = count2
                    listbuffer.clear()
                    flag = false
                    //3 total合并上 模糊区域的 集合v2
                    listTotal ++= aaa
                  }
                }
              }
            }
            listTotal.toList
          }
        }
      }
    }
    resultRDD.flatMap(x=>x)
  }




  //定义方法 处理集合 v
  def handlerList(v:ListBuffer[Tuple2[String,(Long,String,Long)]]):ListBuffer[(String,Long,String,Long)]={
    //获取 v中的svc的 种类，以及最小开始时间stime，和最大结束时间etime ，最终取模，划分时间段
    val V=v.toList
    val length=V.size  //获取数组长度
    val mac=V(0)._1
    //取Array 的种类的个数
    val distinctedSVC=V.map(x=>x._2._2).distinct
    var v2=new scala.collection.mutable.ListBuffer[(String,Long,String,Long)]
    //如果是 1个元素，那么就 直接put
    if(length==1){
      //生成返回值
      v2.+=((mac,V(0)._2._1,V(0)._2._2,V(0)._2._3))
    } else{
      val count=distinctedSVC.size
      var Mintime=V(0)._2._1  //最小时间
      val Maxtime=V(length-1)._2._3  //最大时间
      val interval = (Maxtime-Mintime)/count //计算每一份的时间
      for (i <- distinctedSVC){
        //装进 v2
        v2 .+= ((mac,Mintime,i,(Mintime+interval)))
        Mintime=Mintime+interval
      }
    }
    v2
  }


object TimeAcculumatorParam extends AccumulatorParam[Long] {
  override def addInPlace(r1: Long, r2: Long): Long = {
    if (r1 > r2) r1 else r2
  }
  def zero(initialValue: Long): Long = {
    initialValue
  }
}

}


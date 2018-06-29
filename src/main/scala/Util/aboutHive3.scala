package Util

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer

/**
 * Created by Administrator on 2018/5/29.
 */
object aboutHive3 {

  // 处理trace2
  def handlerTrace3(inputTableRDD:RDD[(String,(Long,String))])={
    val rddGroupBymac = inputTableRDD.groupByKey().mapValues(x=>x.toList.sortWith(_._1<_._1))
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
        } else {
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
          var listbuffer=new scala.collection.mutable.ListBuffer[Tuple2[String,(Long,Long)]]
          var listTotal=new scala.collection.mutable.ListBuffer[(String,Long,Long)]
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
            val length = list.size
            if (list.nonEmpty ) {
              //获取第一组元素，并将第一个元素 put
              tuple = list(0)
              old_stime = tuple._1
              old_svc = tuple._2
              old_etime = tuple._3
              listbuffer += Tuple2 ( old_svc, (old_stime, old_etime))
              //遍历元素，寻找 符合条件的元素到 v中进行处理  或者 直接put到表里面去
              if (length>1) {
                for (i <- 1 until length) {
                  current_stime = list(i)._1
                  current_svc = list(i)._2
                  current_etime = list(i)._3
                  //进行条件判断,添加元素到 v ,否则不满足条件的 形成断点
                  if ((current_etime - current_stime) < 60 && (current_stime - old_etime) < 180 ) {
                    listbuffer += Tuple2 (current_svc, (current_stime, current_etime))
                  } else {
                    //就认为 断点已经结束
                    //处理掉之前的数据 listBuffer，total添加数据
                    val aaa = handlerList3(listbuffer)
                    listTotal ++= aaa
                    listbuffer.clear()
                    listbuffer += Tuple2(current_svc,(current_stime, current_etime))
                  }
                  old_stime = current_stime
                  old_svc = current_svc
                  old_etime = current_etime
                }
              }
              listTotal ++= handlerList3(listbuffer)
            }

            (MAC,listTotal.toList)
          }
        }
      }
    }

    //3. 对模糊区域结果数据 在按照 2h规则 合并一遍
    val resultRDD2=resultRDD.mapPartitions { x => x.map {
      y => {
        var list3=new scala.collection.mutable.ListBuffer[(String,Long,Long)]
        // 获取 每个mac分组的 起始stime svc
        val mac = y._1
        val list = y._2
        var svc: String = list(0)._1
        var stime: Long = list(0)._2
        var etime: Long = list(0)._3
        val size = list.size
        // 处理前 size-1 个元素
        if (size == 1) {
          list3 += Tuple3(svc, stime, etime)
        } else {
          for (i <- 1 until size) {
            // 断点开始，重新赋值，break到 下一轮
            if (list(i)._1.equalsIgnoreCase(svc)) {
              // 判断时间 <2小时  单位是 s
              if (list(i)._3 - etime < 7200) {
                etime = list(i)._3
              }else{
                //就用它作为离开时间，直接添加断点，加到v里面
                list3 += Tuple3(svc, stime, etime)
                svc = list(i)._1
                stime = list(i)._2
                etime = list(i)._3
              }
            }
            // 判断 不是同一个 svc , 直接添加断点,并重新赋值 stime svc etime
            else {
              list3 += Tuple3(svc, stime, etime)
              svc = list(i)._1
              stime = list(i)._2
              etime = list(i)._3
            }
          }
          //把最后一组 数据 加到 v里 stime svc etime
          list3 += Tuple3(svc, stime, etime)
        }
        (mac,list3.toList)
      }
    }
    }

    resultRDD2.flatMap(x=>x._2.map(y=>(x._1,y._2,y._1,y._3)))
  }

  //定义方法 处理集合 v
  def handlerList3(v:ListBuffer[Tuple2[String,(Long,Long)]]):ListBuffer[(String,Long,Long)]={
    //获取 v中的svc的 种类，以及最小开始时间stime，和 持续时间 ，按照时间分量 划值
    val length=v.size  //获取数组长度

    var v2=new scala.collection.mutable.ListBuffer[(String,Long,Long)]
    //如果是 1个元素，那么就 直接put
    if(length < 3){
      //生成返回值
      v2 = v.map(x=>(x._1,x._2._1,x._2._2))
    } else{
      val as=v.map(x=>(x._1,x._2._1,(x._2._2-x._2._1))).groupBy(x=>x._1)
        .map(x=>((x._2.minBy(_._2)),x._2)).map(x=>(x._1._1,x._1._2,x._2.map(y=>y._3).sum)).toArray
        .sortWith(_._2<_._2)

      var stime=as(0)._2
      for (x <- as){
         if (stime< x._2) {
           stime=x._2
         }

          var etime=stime+x._3
          v2 .+= ((x._1,stime,etime))

          stime=etime
      }
    }
    v2
  }

  def main(args: Array[String]) {
    val v=new ListBuffer[Tuple2[String,(Long,Long)]]
    v .+=(Tuple2("A",(100L,102L)))
    v .+=(Tuple2("B",(104L,108L)))
    v .+=(Tuple2("A",(50L,60L)))
    v .+=(Tuple2("C",(48L,50L)))
    v .+=(Tuple2("B",(112L,120L)))
    v .+=(Tuple2("A",(130L,140L)))
    v .+=(Tuple2("C",(142L,150L)))
    v .+=(Tuple2("A",(160L,180L)))

    val as=handlerList3(v)
    as.foreach(println)


  }
}

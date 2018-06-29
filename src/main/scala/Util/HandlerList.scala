package probd.mactrace

import scala.collection.mutable.ListBuffer

/**
 * Created by probdViewSonic on 2017/1/16.
 */
object HandlerList{

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
}


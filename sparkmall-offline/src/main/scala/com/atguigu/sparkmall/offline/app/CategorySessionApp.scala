package com.atguigu.sparkmall.offline.app

import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.JDBCUtil
import com.atguigu.sparkmall.offline.bean.{CategoryCountInfo, CategorySession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CategorySessionApp {

  def statCategoryTop10Session(spark: SparkSession, categoryCountInfoList: List[CategoryCountInfo],
                               userVisitActionRDD: RDD[UserVisitAction], taskId: String) = {
    //1. 需求1,只需要拿出CategoryId
    val top10CategoryId = categoryCountInfoList.map(_.categoryId)

    //2.从 RDD[UserVisitAction].filter(cids.contains(_.CategoryId)) 过滤出只有top10品类的用户的行为日志
    val filteredUserVisitActionRDD = userVisitActionRDD.filter(userAction => {
      top10CategoryId.contains(userAction.click_category_id.toString)
    })

    //3. 类型转换
    val categoryCountRDD = filteredUserVisitActionRDD.map {
      action => ((action.click_category_id, action.session_id), 1)
    }.reduceByKey(_ + _).map {
      case ((cid, sid), count) => (cid, (sid, count))
    }

    //4. 按照 key 进行分组
    val groupedCategoryCountRDD = categoryCountRDD.groupByKey()
    val categroySessionRDD: RDD[CategorySession] = groupedCategoryCountRDD.flatMap {

      // 对迭代器中的数据排序,取前十
      case (cid, ite) => {
        ite.toList.sortBy(_._2)(Ordering.Int.reverse).take(10).map {
          case (sid, count) => CategorySession(taskId, cid.toString, sid, count)
        }
      }
    }

    //5. 写入到mysql ,数据量很小,可以将数据拉取到Driver,然后再写入
    val csArray = categroySessionRDD.map(
      cs => Array(cs.taskId, cs.categoryId, cs.sessionId, cs.clickCount)).collect

    JDBCUtil.executeUpdate("truncate table category_top10_session_count",null)
    JDBCUtil.executeBatchUpdate("insert into category_top10_session_count values(?,?,?,?)",csArray)


  }

}

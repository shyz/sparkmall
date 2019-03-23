package com.atguigu.sparkmall.offline

import java.util.UUID

import com.alibaba.fastjson.JSON
import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.ConfigurationUtil
import com.atguigu.sparkmall.offline.app.CategoryTop10App
import com.atguigu.sparkmall.offline.bean.Condition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object OfflineApp {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "atguigu")
    // 1. 把用户行为的数据读取出来, 放在 RDD 中
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("MockOffline")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "hdfs://hadoop101:9000/user/hive/warehouse")
      .getOrCreate()
    val taskId = UUID.randomUUID().toString
    // 根据条件过滤出来需要的 UserVisitAction
    val userVisitActionRDD = readUserVisitActionRDD(spark, readCondition)

    println("任务1:开始")
    CategoryTop10App.statCategoryTop10(spark, userVisitActionRDD, taskId)
    println("任务1:结束")
    //userVisitActionRDD.cache  //做缓存
    //userVisitActionRDD.persist()   //设置保存级别,持久化

    // 需求1: 统计品类的top10

    //val categoryTop10 = CategoryTop10App.statCategoryTop10(spark, userVisitActionRDD)

    // 需求2: 统计top10 品类中的 活跃度 top10 的 session
    //CategorySessionApp.statCategoryTop10Session(spark,categoryTop10,userVisitActionRDD,taskId)

  }

  /**
    * 根据传入的条件, 来读取用户行为的数据
    *
    * @param spark
    * @param condition
    */
  def readUserVisitActionRDD(spark: SparkSession, condition: Condition): RDD[UserVisitAction] = {
    // 1. 先有sql语句
    var sql = s"select v.* from user_visit_action v join user_info u on v.user_id=u.user_id where 1=1"
    if (isNotEmpty(condition.startDate)) {
      sql += s" and v.date>='${condition.startDate}'"
    }
    if (isNotEmpty(condition.endDate)) {
      sql += s" and v.date<='${condition.endDate}'"
    }
    if (condition.startAge > 0) {
      sql += s" and u.age >= ${condition.startAge}"
    }
    if (condition.endAge > 0) {
      sql += s" and u.age <= ${condition.endAge}"
    }
    // 2. 执行
    import spark.implicits._
    spark.sql("use sparkmall")
    spark.sql(sql).as[UserVisitAction].rdd
  }

  /**
    * 读取过滤条件
    *
    * @return
    */
  def readCondition(): Condition = {

    //读取配置文件中的的json 字符串
    val conditionStr: String = ConfigurationUtil("conditions.properties").getString("condition.params.json")
    //解析成condition对象
    JSON.parseObject(conditionStr, classOf[Condition])
  }
}

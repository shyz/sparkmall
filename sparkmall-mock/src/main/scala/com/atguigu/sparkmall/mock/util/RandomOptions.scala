package com.atguigu.sparkmall.mock.util


import scala.collection.mutable.ListBuffer

/**
  * 根据提供的值和比重, 来创建RandomOptions对象.
  * 然后可以通过getRandomOption来获取一个随机的预定义的值
  */
object RandomOptions {
  /**
    * apply 方法在用在类和伴生类对象中可以隐式调用
    * (T, Int)* 可变集合,T 可变,int 类型不可变,表示权重
    * @param opts
    * @tparam T
    * @return
    */
  def apply[T](opts: (T, Int)*) ={
    val randomOptions = new RandomOptions[T]()
    randomOptions.totalWeight = (0 /: opts)(_ + _._2) // 计算出来总的比重
    opts.foreach{
      // ++= 连接两个集合
      case (value, weight) => randomOptions.options ++= (1 to weight).map(_ => value)
    }
    randomOptions
  }


  def main(args: Array[String]): Unit = {
    // 测试
    val opts = RandomOptions(("张三", 1), ("李四", 3))
    //println(opts.options)
    (0 to 50).foreach(_ => println(opts.getRandomOption()))
  }
}

/**
  *
  * @tparam T
  */
class RandomOptions[T]{
  // 总的比重
  var totalWeight: Int = _
  var options = ListBuffer[T]()

  /**
    * 获取随机的 Option 的值
    * @return
    */
  def getRandomOption() = {
    options(RandomNumUtil.randomInt(0, totalWeight - 1))
  }
}
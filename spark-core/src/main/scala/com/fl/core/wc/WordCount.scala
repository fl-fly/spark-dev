package com.fl.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: fl.lei
 * @CreateTime: 2022/5/4 15:54
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    // 创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    // 创建spark上下文环境对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 读取文件
    val fileRDD: RDD[String] = sc.textFile("E:\\workspace\\spark-dev\\spark-core\\src\\main\\resources\\file\\fl")
    // 将文件进行分词
    val wordCount: RDD[String] = fileRDD.flatMap(_.split(" "))
    // 将单词分组
    val wordGroup: RDD[(String, Iterable[String])] = wordCount.groupBy(word => word)
    // 将转换结构后的数据按照相同的单词进行分组聚合

    // 对分组数据进行转换
    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    // 将数据聚合结果采集到内存中
    val array: Array[(String, Int)] = wordToCount.collect()

    array.foreach(println)

    // 关闭连接
    sc.stop();

  }

}

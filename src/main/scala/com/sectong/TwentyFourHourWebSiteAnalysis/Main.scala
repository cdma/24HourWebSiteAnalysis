package com.sectong.TwentyFourHourWebSiteAnalysis

import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.SaveMode

/**
 * Apache Access Log Analyzer
 */
object Main {
  val WINDOW_LENGTH = new Duration(86400 * 1000)
  val SLIDE_INTERVAL = new Duration(10 * 1000) //执行间隔

  def main(args: Array[String]) {
    //    if (args.length < 4) {
    //      System.err.println("Usage: LogAnalyzerApacheAccessLog <zkQuorum> <group> <topics> <numThreads>")
    //      System.exit(1)
    //    }

    val sparkConf = new SparkConf().setAppName("Logyun Analyzer: Apache Access Log").setMaster("local[4]") //本地测试运行
    sparkConf.set("spark.sql.hive.thriftServer.singleSession", "true")

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, SLIDE_INTERVAL)
    val sqlContext = new SQLContext(sc)

    val hiveContext = new HiveContext(sc)

    //    val Array(zkQuorum, group, topics, numThreads) = args // 启动参数初始化
    val Array(zkQuorum, group, topics, numThreads) = Array("hadoop2", "groups24", "syslog", "1") // 本地测试运行

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap //转换topic为topicMap

    val logLinesDStream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2) //获取Kafka数据

    val accessLogsDStream = logLinesDStream.flatMap(line => {
      try {
        Some(ApacheAccessLog.parseLogLine(line)) //执行解析
      } catch {
        case e: RuntimeException => None //如果解析失败，空值
      }
    }).cache() //解析并缓存

    val windowDStream = accessLogsDStream.window(WINDOW_LENGTH, SLIDE_INTERVAL)

    windowDStream.foreachRDD(line => {
      if (line.isEmpty()) {
        println("No access logs received in this time interval")
      } else {
        val df = hiveContext.createDataFrame(line) //创建DataFrame
        df.registerTempTable("logs")
        val top10SrcIp24Hour = hiveContext.sql("SELECT ipAddress, COUNT(*) as count FROM logs GROUP BY ipAddress ORDER BY count DESC LIMIT 100")
        top10SrcIp24Hour.write.mode(SaveMode.Overwrite).parquet("/tmp/apacheaccesslog/") //写入Parquet文件
        top10SrcIp24Hour.registerTempTable("top10")
      }
    })

    hiveContext.setConf("hive.server2.thrift.port", "19898")
    HiveThriftServer2.startWithContext(hiveContext)

    ssc.start() //启动ssc
    ssc.awaitTermination()

  }
}
package com.ydl

import org.apache.spark.{SparkConf, SparkContext}
import it.nerdammer.spark.hbase._

/**
  * SparkWithHbaseDemo
  *
  * Created by ydl on 2018/1/16.
  */
class SparkWithHbaseDemo {

}

object SparkWithHbaseDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
                .setAppName("sparkWhihHbase")
                .setMaster("local[2]")
        //两种编码形式配置hbase信息
        //conf.set("spark.hbase.host", "app-dev1-xyplus-a1")
        val sc = new SparkContext(conf)
        sc.hadoopConfiguration.set("spark.hbase.host", "app-dev1-xyplus-a1")

//        write(sc)
        read(sc)
    }

    /**
      * 写数据
      *
      * @param sc
      */
    def write(sc: SparkContext): Unit = {
        val rdd = sc.parallelize(1 to 10000).map(i => (i.toString, i + 1, "Hello"))
        rdd.foreach(println)
        rdd.toHBaseTable("t_book").toColumns("column1", "column2").inColumnFamily("base").save()
    }

    def read(sc: SparkContext): Unit = {
        val hBaseRDD = sc.hbaseTable[(String, Int, String)]("t_book")
                .select("column1", "column2")
                .inColumnFamily("base")
        hBaseRDD.foreach(println)
    }
}
package wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object wordCount01 {
  def main(args: Array[String]): Unit = {
    //建立链接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //1.读取文件,行读取
    val lines: RDD[String] = sc.textFile(path = "datas")
    //2.拆分成单个单词，扁平化,切分成单个单词
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //3.对分组后数据进行转换 rdd(key, values(分组后的集合))
    val wordGroup: RDD[(String , Iterable[String])] = words.groupBy(word => word)
    //4.分组后数组进行转换,统计单词个数

    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println);

    //关闭连接
    sc.stop();
  }
}

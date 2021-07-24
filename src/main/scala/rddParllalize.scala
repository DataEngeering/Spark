import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level



object rddParllalize extends App{
  val conf = new SparkConf()
  conf.set("spark.app.name","RDD Parallalize")
  conf.set("spark.master","local[*]")
  
  val spark = SparkSession.builder().config(conf).getOrCreate()
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val logger = Logger.getLogger(getClass.getName)
  
  val sc = spark.sparkContext
  
  val list =List(1,2,3,4)
  
  val rdd =sc.parallelize(list)
  rdd.collect().foreach(println)
  
 logger.info(s"no of partition ${rdd.getNumPartitions}")
 
 logger.info(s"first value ${rdd.first()}")
 logger.info("defaultParallalism" + sc.defaultParallelism)
 
 logger.info("defaul min parallalism" + sc.defaultMinPartitions)
}
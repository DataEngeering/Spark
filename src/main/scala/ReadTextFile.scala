

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level


object ReadTextFile extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR);
  
  val logger = Logger.getLogger(getClass.getName)
  
  
  val conf = new SparkConf()
  conf.set("spark.app.name", "Read Text Files")
  conf.set("spark.master", "local[*]")
  
  val spark = SparkSession.builder().config(conf).getOrCreate()
  val sc = spark.sparkContext;
  
  val baseRDD = sc.textFile("D:\\shiva\\POC\\SoumyaGit\\testdata\\ReadFileData\\*");
  
  logger.info(baseRDD.collect().foreach(println));
  
  val multiFileRDD = sc.textFile("D:\\shiva\\POC\\SoumyaGit\\testdata\\ReadFileData\\invalid.txt,D:\\shiva\\POC\\SoumyaGit\\testdata\\ReadFileData\\text01.txt");
  logger.info("Reading from multiple files .................................");
  logger.info(multiFileRDD.collect().foreach(println));
  
  var wholeTextRDD = sc.wholeTextFiles("D:\\shiva\\POC\\SoumyaGit\\testdata\\ReadFileData\\*")
  wholeTextRDD.collect().foreach(x=> {
    logger.info(s"File Name :: ${x._1}")
    logger.info(s"File Content :: ${x._2}")
  })
  
  var wholeTextRDD1 = sc.wholeTextFiles("D:\\shiva\\POC\\SoumyaGit\\testdata\\ReadFileData\\invalid.txt,D:\\shiva\\POC\\SoumyaGit\\testdata\\ReadFileData\\text01.txt")
  logger.info("multiple files with whole file ............................")
  wholeTextRDD.collect().foreach(x=> {
    logger.info(s"File Name :: ${x._1}")
    logger.info(s"File Content :: ${x._2}")
  })
  
  
}
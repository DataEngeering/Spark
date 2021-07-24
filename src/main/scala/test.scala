import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object test extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

	val conf = new SparkConf()
	conf.set("spark.app.name","AmbigiousName")
	conf.set("spark.master","local[2]")

	val spark = SparkSession.builder()
	.config(conf)
	.getOrCreate()
	
	val custDf = spark.read
	.format("csv")
	.option("path", "D:\\Soumya\\Scala\\Week12\\customers-201025-223502.csv")
	.option("inferSchema", true)
	.option("header", true)
	.load
	
	val custDf_new = custDf.withColumnRenamed("customer_id", "order_customer_id")
	
	val ordDf = spark.read
	.format("csv")
	.option("path", "D:\\Soumya\\Scala\\Week12\\orders-201025-223502.csv")
	.option("inferSchema", true)
	.option("header", true)
	.load
	
	val cont = ordDf.join(custDf_new, ordDf.col("order_customer_id")=== custDf_new.col("order_customer_id"), "inner")
	.where(ordDf.col("order_status")==="CLOSED")
	.sort(custDf_new.col("order_customer_id"))
	//.show(false)
	
	cont.show()
	
	
	
	
	
	
	spark.close()
}
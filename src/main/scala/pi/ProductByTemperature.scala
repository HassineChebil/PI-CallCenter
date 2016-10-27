
package pi


import com.databricks.spark.csv
import org.apache.spark.sql
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._




object ProductByTemperature {
  
  def main(args : Array[String]){
    val conf = new SparkConf().setAppName("ProductByTemperature")
    val sc = new SparkContext(conf)
    
    val sqlcontext = new SQLContext(sc);
    
    val df = sqlcontext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
	  .option("delimiter", ";")
    .option("inferSchema", "true") // Automatically infer data types
    .load("C:/pi-spark/Datasets/US_Sales_Products_2012-14.csv")
    
    
    val tp = sqlcontext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
	  .option("delimiter", ";")
    .option("inferSchema", "true") // Automatically infer data types
    .load("C:/pi-spark/Datasets/TemperatureByState.csv")
    
    
    
    val dfLAUSers = df.select("state","Product type","Product","Year",
        "Quantity").filter(df("state") === "LA" )
    val LAtemp = tp.select ("AverageTemperature","state","year").filter(tp("state") === "LA" )
    
    
    dfLAUSers.registerTempTable("product")
    LAtemp.registerTempTable("temp")
    
   
    
   val joined = sqlcontext.sql("""
    SELECT *
    FROM product p JOIN temp t
    ON p.state = t.state
    
   
     """)
    
     val result = joined.filter(joined("year") >= 2012 ).groupBy("t.state","Product type","Product","year","Quantity","AverageTemperature").sum("Quantity")
    val analys = result.filter(joined("year") >= 2012 ).groupBy("t.state","Product type","Product","year","sum(Quantity)").max("AverageTemperature") 
     analys.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("C:/pi-spark/Datasets/output/ProductByTemperatureByQunt")
       analys.toJSON.saveAsTextFile("C:/pi-spark/Datasets/output/ProductByTemperatureByQuntjson")

     /* result.show(100)*/
          /*joined.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("C:/pi-spark/Datasets/output/TestTest.csv")*/

         
    
    sc.stop()
      
  }
  
}
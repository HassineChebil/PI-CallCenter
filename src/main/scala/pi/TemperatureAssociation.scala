package pi


import com.databricks.spark.csv
import org.apache.spark.sql
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object TemperatureAssociation {
  
  def main(args : Array[String]){
    val conf = new SparkConf().setAppName("TemperatureAssociation")
    val sc = new SparkContext(conf)
    
    val sqlcontext = new SQLContext(sc);
    
    val df = sqlcontext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
	  .option("delimiter", ";")
    .option("inferSchema", "true") // Automatically infer data types
    .load("C:/Users/mehdikarray/Desktop/mehdi/Esprit/5eme/Pi_social_analytics/dataset/US_persons.csv")
    
    
    val tp = sqlcontext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
	  .option("delimiter", ";")
    .option("inferSchema", "true") // Automatically infer data types
    .load("C:/Users/mehdikarray/Desktop/mehdi/Esprit/5eme/Pi_social_analytics/dataset/US_TemperaturesByState.csv")
    
    val pr = sqlcontext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
	  .option("delimiter", ";")
    .option("inferSchema", "true") // Automatically infer data types
    .load("C:/Users/mehdikarray/Desktop/mehdi/Esprit/5eme/Pi_social_analytics/dataset/US_Sales_Products.csv")
    
    val dfLAUSers = df.select("first_name","state")
    val LAtemp = tp.select ("dt","AverageTemperature","State").filter(tp("dt") === "01/09/2013")
    
    dfLAUSers.registerTempTable("user")
    LAtemp.registerTempTable("temp")
    
    val joined = sqlcontext.sql("""
    SELECT first_name, AverageTemperature
    FROM user a JOIN temp s
    ON a.state = s.State""")
    
    
    
    val hotemp = joined.select ("first_name","AverageTemperature").filter(joined("AverageTemperature") > "25" && joined("AverageTemperature") < "30")
    val produithot = pr.select("Product type","Product").filter(pr("Product type") === "Sunscreen")
    
    hotemp.registerTempTable("userhot")
    produithot.registerTempTable("prodhot")
    
    val joinedproduserhot = sqlcontext.sql("""
    SELECT first_name, Product
    FROM userhot a JOIN prodhot s
    """)
    
    joinedproduserhot.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("C:/Users/mehdikarray/Desktop/mehdi/Esprit/5eme/Pi_social_analytics/UserProductscsv")
    joinedproduserhot.toJSON.saveAsTextFile("C:/Users/mehdikarray/Desktop/mehdi/Esprit/5eme/Pi_social_analytics/UserProductsjson")
    joinedproduserhot.show(200)
    
    sc.stop()
      
  }
  
}


package pi

import com.databricks.spark.csv
import org.apache.spark.sql
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object PersonalityAssociation {
  
  def main(args : Array[String]){
    val conf = new SparkConf().setAppName("PersonalityAssociation")
    val sc = new SparkContext(conf)
    
    val sqlcontext = new SQLContext(sc);
    
    val df = sqlcontext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
	.option("delimiter", ";")
    .option("inferSchema", "true") // Automatically infer data types
    .load("P:/esprit/5TWIN/PI/Datasets/US_persons.csv")
    
    val dfLAUSers = df.select("first_name","state").filter(df("state") === "LA")
    
    dfLAUSers.show()
    
    sc.stop()
    
    
    
  }
  
}
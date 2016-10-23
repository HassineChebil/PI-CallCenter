

package pi

import com.databricks.spark.csv
import org.apache.spark.sql
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._


object CentralizeResearch {
  
  def main(args : Array[String]){
    var ch = ""
    do{
      println("Bienvenue")
      println("choisir 0: Quitter ")
      println("choisir 1: Filter par etat")
      println("choisir 2: Filter par client")
      println("choisir 3: Filter par produit")
      ch = readLine()
    }while((ch.toInt < 0) || (ch.toInt > 3))
      var st = ""; 
     if(ch == "1" ){
        println("Donnez un etat")
        st = readLine()
     }else
     if(ch == "2" ){
        println("Donnez son email")
        st = readLine()
     }else
     if(ch == "3" ){
        println("Donnez un Type de produit")
        st = readLine()
     }else{
      println("bye bye")
      System.exit(1);   
    }  
      
    val conf = new SparkConf().setAppName("CentralizeResearch")
    val sc = new SparkContext(conf)
    
    val sqlcontext = new SQLContext(sc);
    
    val df = sqlcontext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
	.option("delimiter", ";")
    .option("inferSchema", "true") // Automatically infer data types
    .load("D:/Formation/(esprit)5TWIN/PI/docs/US_persons.csv")
    
    val dfLAUSers = df.select("first_name","last_name","Age","Gender","email","state")
    
    val df2 = sqlcontext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
	.option("delimiter", ";")
    .option("inferSchema", "true") // Automatically infer data types
    .load("D:/Formation/(esprit)5TWIN/PI/docs/US_Sales_Products_2012-14.csv")
    
    val dfLAProducts = df2.groupBy("state","Product type").sum("Quantity")
    
    if(ch == "1"){
      val res = dfLAProducts.filter(df2("state") === st).sort(desc("sum(Quantity)")).limit(1)
      val res2 = dfLAUSers.filter(df("state") === st) 
      res2.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("D:/Formation/(esprit)5TWIN/PI/output/Employee.csv")
      res.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("D:/Formation/(esprit)5TWIN/PI/output/Products.csv")

    }else
    if(ch == "2"){
      val res2 = dfLAUSers.filter(df("email") === st)
      val res = dfLAProducts.sort("state","Product type").filter(df2("state") === res2.first()(5))
      res2.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("D:/Formation/(esprit)5TWIN/PI/output/Employee.csv")
      res.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("D:/Formation/(esprit)5TWIN/PI/output/Products.csv")
    }else
    if(ch == "3"){
      val res = dfLAProducts.filter(df2("Product type")=== st).sort(desc("sum(Quantity)")).limit(1)
      val res2 = dfLAUSers.filter(df("state") === res.first()(0)) 
      res2.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("D:/Formation/(esprit)5TWIN/PI/output/Employee.csv")
      res.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("D:/Formation/(esprit)5TWIN/PI/output/Products.csv")    
    }else{
      println("bye bye")
      System.exit(1);   
    }
    
    sc.stop()
    
    
    
  }
  
}
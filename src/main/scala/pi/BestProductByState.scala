
package pi


import com.databricks.spark.csv
import org.apache.spark.sql
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._




object BestProductByState {
  
  def main(args : Array[String]){
    
    var ch = ""
    do{
      println("******************************************************************************")
      println("Bienvenue")
      println("******************************************************************************")
      println("Ceci vous permettra de connaitre ou est ce que le produit est le plus vendu")  
      println("******************************************************************************")
      println("choisir 1: Pour entrer votre produit")
      
      println("choisir 0: Quitter ")
      println("******************************************************************************")
      ch = readLine()
    }while((ch.toInt < 0) || (ch.toInt > 3))
      var pr = ""; 
     if(ch == "1" ){
        println("******************************************************************************")
        println("Donner un produit")
        pr = readLine()
     }else{
      println("******************************************************************************") 
      println("bye bye")
      println("*******************f***********************************************************")
      System.exit(1);   
    }  
    println("******************************************************************************")  
    
    
    val conf = new SparkConf().setAppName("BestProductByState")
    val sc = new SparkContext(conf)
    
    val sqlcontext = new SQLContext(sc);
    
    val df = sqlcontext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
	  .option("delimiter", ";")
    .option("inferSchema", "true") // Automatically infer data types
    .load("C:/pi-spark/Datasets/US_Sales_Products_2012-14.csv")
    
    
   
   
    
    val dfBest = df.groupBy("state","Product type","Product","Year","Quantity").sum("Quantity")
    
   
    if(ch == "1"){
      val prod = dfBest.filter(df("Product") === pr).sort(desc("sum(Quantity)")).limit(1)
		  var res = df.filter(df("Product") === pr && df("Product type") === prod.first()(1)).groupBy("state","Product type","Product","Year").sum("Quantity").sort(desc("sum(Quantity)"))
      
      res.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("C:/pi-spark/Datasets/output/BestProductsByState")

    }
  
    
    /*val best = dfProds.filter(dfProds("year") >= 2012 ).groupBy("t.state","Product type","Product","year","sum(Quantity)").max("AverageTemperature")*/ 
         /*dfBest.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("C:/pi-spark/Datasets/output/Bestcsv")*/

   /* result.show(100)*/
          /*joined.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("C:/pi-spark/Datasets/output/TestTest.csv")*/

         
    
    sc.stop()
      
  }
  
}
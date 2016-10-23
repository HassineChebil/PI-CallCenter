

package pi

import com.databricks.spark.csv
import org.apache.spark.sql
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object PersonalityAssociation {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PersonalityAssociation")
    val sc = new SparkContext(conf)

    val sqlcontext = new SQLContext(sc);

    val df = sqlcontext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("delimiter", ",")
      .option("inferSchema", "true") // Automatically infer data types
      .load("P:/esprit/5TWIN/PI/Datasets/US_persons.csv")

    println("please enter your name")
    val name = readLine()
    println("Welcome " + name)
    println("shall we start: write (ok) to start; anything else well ...")
    val answer = readLine();

    if (answer == "ok") {
      println("so what you want to do today: ")
      println("1- I have a client that i want to associate to a perfect employee. Press(1)")
      println("2- I have an employee that can't communicate well can you find him the perfect association. Press(2)")
      val choice = readLine();
      if (choice.toInt == 1) {

        val dfEmployees = df.select("first_name", "last_name", "Age", "EducationField", "Gender", "state", "type").filter(df("type") === "emp")
        println("*********you choosed 1************")
        println("How do you want to associate him/her: ")
        println("1- According to the state: press(1)")
        println("2- According to the state & Educational Field: press(2)")
        println("3- According to the state & Educational Field & Gender: press(3)")
        val choice = readLine()
        if (choice.toInt == 1) {
          println("****choose a state***")
          val state = readLine()
          val dfEmployeesByState = dfEmployees.select("first_name", "last_name", "Age", "EducationField", "Gender", "state", "type").filter(dfEmployees("state") === state)
          if (dfEmployeesByState.take(1).length != 0) {
            dfEmployeesByState.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("P:/esprit/5TWIN/PI/Datasets/output/EmployeesByState.csv")
          } else {
            println("******************************************************************************")
            println("NO MATCH FOUND!!! TRY ANOTHER TIME !! YOU CAN'T LOOSE A CLIENT")
            println("******************************************************************************")

          }
        } else if (choice.toInt == 2) {
          println("****choose a state***")
          val state = readLine()
          println("****choose an Educational Field***")
          val educationalfield = readLine()
          val dfEmployeesByStateAndEducationalField = dfEmployees.select("first_name", "last_name", "Age", "EducationField", "Gender", "state", "type").filter(dfEmployees("state") === state && dfEmployees("EducationField") === educationalfield)
          if (dfEmployeesByStateAndEducationalField.take(1).length != 0) {
            dfEmployeesByStateAndEducationalField.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("P:/esprit/5TWIN/PI/Datasets/output/EmployeesByStateAndEducationalField.csv")
          } else {
            println("******************************************************************************")
            println("NO MATCH FOUND!!! TRY ANOTHER TIME !! YOU CAN'T LOOSE A CLIENT")
            println("******************************************************************************")
          }
        } else {
          println("****choose a state***")
          val state = readLine()
          println("****choose an Educational Field***")
          val educationalfield = readLine()
          println("****choose a Gender***")
          val gender = readLine()
          val dfEmployeesByStateAndEducationalFieldAndGender = dfEmployees.select("first_name", "last_name", "Age", "EducationField", "Gender", "state", "type").filter(dfEmployees("state") === state && dfEmployees("EducationField") === educationalfield && dfEmployees("Gender") === gender)
          if (dfEmployeesByStateAndEducationalFieldAndGender.take(1).length != 0) {
            dfEmployeesByStateAndEducationalFieldAndGender.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("P:/esprit/5TWIN/PI/Datasets/output/EmployeesByStateAndEducationalFieldAndGender.csv")
          } else {
            println("******************************************************************************")
            println("NO MATCH FOUND!!! TRY ANOTHER TIME !! YOU CAN'T LOOSE A CLIENT")
            println("******************************************************************************")
          }
        }
        sc.stop()

      } else {
        println("*********you choosed 2************")
        val dfClients = df.select("first_name", "last_name", "Age", "EducationField", "Gender", "state", "type").filter(df("type") === "client")
        println("How do you want to associate him/her: ")
        println("1- According to the state: press(1)")
        println("2- According to the state & Educational Field: press(2)")
        println("3- According to the state & Educational Field & Gender: press(3)")
        val choice = readLine()
        if (choice.toInt == 1) {
          println("****choose a state***")
          val state = readLine()
          val dfClientsByState = dfClients.select("first_name", "last_name", "Age", "EducationField", "Gender", "state", "type").filter(dfClients("state") === state)
          if (dfClientsByState.take(1).length != 0) {
            dfClientsByState.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("P:/esprit/5TWIN/PI/Datasets/output/ClientsByState.csv")
          } else {
            println("******************************************************************************")
            println("NO MATCH FOUND!!! TRY ANOTHER TIME !! OR FIRE HIM AND RECRUTE A BETTER MATCH")
            println("******************************************************************************")
          }
        } else if (choice.toInt == 2) {
          println("****choose a state***")
          val state = readLine()
          println("****choose an Educational Field***")
          val educationalfield = readLine()
          val dfClientsByStateAndEducationalField = dfClients.select("first_name", "last_name", "Age", "EducationField", "Gender", "state", "type").filter(dfClients("state") === state && dfClients("EducationField") === educationalfield)
          if (dfClientsByStateAndEducationalField.take(1).length > 0) {
            dfClientsByStateAndEducationalField.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("P:/esprit/5TWIN/PI/Datasets/output/ClientsByStateAndEducationalField.csv")
          } else {
            println("******************************************************************************")
            println("NO MATCH FOUND!!! TRY ANOTHER TIME !! OR FIRE HIM AND RECRUTE A BETTER MATCH")
            println("******************************************************************************")
          }
        } else {
          println("****choose a state***")
          val state = readLine()
          println("****choose an Educational Field***")
          val educationalfield = readLine()
          println("****choose a Gender***")
          val gender = readLine()
          val dfClientsByStateAndEducationalFieldAndGender = dfClients.select("first_name", "last_name", "Age", "EducationField", "Gender", "state", "type").filter(dfClients("state") === state && dfClients("EducationField") === educationalfield && dfClients("Gender") === gender)
          if (dfClientsByStateAndEducationalFieldAndGender.take(1).length > 0) {
            dfClientsByStateAndEducationalFieldAndGender.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("P:/esprit/5TWIN/PI/Datasets/output/ClientsByStateAndEducationalFieldAndGender.csv")
          } else {
            println("******************************************************************************")
            println("NO MATCH FOUND!!! TRY ANOTHER TIME !! OR FIRE HIM AND RECRUTE A BETTER MATCH")
            println("******************************************************************************")
          }
        }

        sc.stop()
      }
      /*

    val df = sqlcontext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("delimiter", ";")
      .option("inferSchema", "true") // Automatically infer data types
      .load("P:/esprit/5TWIN/PI/Datasets/US_persons.csv")

    //df.registerTempTable("EmployeesState")

    //val dfEmployeesState = sqlcontext.sql("select count(*),state from EmployeesState GROUP BY state,first_name")

    //val dfEmployees = df.collect().map(t => When() println("booooooooooooo:::: " + t) )

    //dfEmployeesState.show()

    val dfEmployeeByState = df.groupBy("state").count()
    val dfEmployeeByEducationField = df.groupBy("EducationField").count()

    dfEmployeeByState.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("P:/esprit/5TWIN/PI/Datasets/output/EmployeeByState.csv")
    dfEmployeeByEducationField.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("P:/esprit/5TWIN/PI/Datasets/output/EmployeeByEducationField.csv")

    sc.stop()*/
    } else {
      println("well bye bye!!!")
    }
  }

}
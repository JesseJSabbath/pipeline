import scala.io.StdIn.readLine
import scala.util.control.Breaks
import DataLoad._
import Queries._
import org.apache.log4j.PropertyConfigurator
import org.apache.hadoop.fs.FileAlreadyExistsException
import java.sql.{Connection, DriverManager}
import scala.Console._
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import java.sql.PreparedStatement
import java.sql.SQLException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark._
import org.apache.spark.sql.functions._
/*import com.coxautodata.vegalite4s.VegaLite  
import com.coxautodata.vegalite4s.renderers.ImplicitRenderers.AutoSelectionRenderer
*/


object DataPipeline  {
  var connection = getConnection
  def main(args: Array[String]): Unit = {
   
    //var connection = getConnection
    Queries.testHello
    DataLoad.dataLoad
    Queries.names
    /*var statement = connection.createStatement
        var rs = statement.executeQuery("SELECT * FROM cdw_sapp_customer")
        while(rs.next) {
            print(rs.getString("first_name") + " ")
            println(rs.getString("last_name"))
        }*/
    mainMenu
    
  
    Queries.purgeDB(connection) //comment out when actually appending new files
    
  }
    
  def mainMenu: Unit = {
    println("M A I N   M E N U")
    var choice = ""
    while (choice != "4") {
      println
      println("1) Transactions Details")
      println("2) Customer Details")
      println("3) Graphs")
      println("4) Exit")
      println
      print("Please select from the options above: ")
      choice = readLine.trim
      if (choice == "1")
        transactionMenu
      else if (choice == "2")
        customerDetailMenu
      else if (choice == "3")
        graphMenu
      else if (choice != "4") {
        println("Invalid selection")
        print("Please select from the options above: ") 
      }
        
     }
    
    println("Exiting program...")
  }

  def transactionMenu: Unit = {
    println
    println("1) Transactions By Zipcode")
    println("2) Transactiona By Type")
    println("3) Transactions By State")
    println("4) Return to Main Menu")
    println
  }

  def customerDetailMenu: Unit = {
    println
    println("1) Display Customer Details")
    println("2) Update Customer Details")
    println("3) Generate Monthly Bill")
    println("4) Display Transaction Range")
    println("5) Return to Main Menu")
    println
  }

  def graphMenu: Unit = {
    var connection = getConnection
    println
    println("1) Print Transactions Graph")
    println("2) Print Customer/State Graph")
    println("3) Print Top 10 Graph")
    println("4) Return to previous menu")
    var choice = readLine.trim
    if (choice == "4")
      mainMenu
    
      

    println


  }
  
  def getConnection: Connection = {
      val dbUrl = "jdbc:mysql://localhost:3306/creditcard_db?autoReconnect=true&useSSL=false"
      val driver = "com.mysql.cj.jdbc.Driver"
      val filePath = "/home/jesse/scala/pipeline/.secret.txt"
      val creds = Source.fromFile(filePath).getLines.toArray
      val username = creds(0)
      val password = creds(1)
      var connection: Connection = DriverManager.getConnection(dbUrl,username,password)
      connection
  }

  def purgeDB(connection:Connection): Unit = {
    var statement = connection.createStatement
    var rs = statement.executeUpdate("truncate cdw_sapp_loan_application")
    rs = statement.executeUpdate("truncate cdw_sapp_customer")
    rs = statement.executeUpdate("truncate cdw_sapp_branch")
    rs = statement.executeUpdate("truncate cdw_sapp_credit_card")
    println("all tables truncated")
  }
  
}


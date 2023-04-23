import scala.io.StdIn.readLine
import scala.util.control.Breaks

object DataPipeline  {
  def main(args: Array[String]): Unit = {
    mainMenu
  }
    
  def mainMenu: Unit = {
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
    println
  }

  def customerDetailMenu: Unit = {
    println
    println("1) Display Customer Details")
    println("2) Update Customer Details")
    println("3) Generate Monthly Bill")
    println("4) Display Transaction Range")
    println
  }

  def graphMenu: Unit = {
    println
    println("1) Print Transactions Graph")
    println("2) Print Customer/State Graph")
    println("3) Print Top 10 Graph")
    println
  }
  
}


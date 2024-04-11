import java.sql.{Connection, DriverManager}
import scala.io.Source
import java.sql.PreparedStatement
import java.sql.SQLException

object Queries {
    var connection = getConnection
    var statement = connection.createStatement
    def testHello {
        println("Hello from the Query File!")
    }

    def names {
        //var connection = getConnection
        //var statement = connection.createStatement
        var rs = statement.executeQuery("SELECT * FROM cdw_sapp_customer limit 10")
        while(rs.next) {
            print(rs.getString("first_name") + " ")
            println(rs.getString("last_name"))
        }
    }

    //Transaction details queries
    def transactionsByZip() {
        println("implement transaction by customers in a given zip for given month and year, ordered by day desc")
    }

    def transactionsByType() {
        println("implement total number and total val of transactions by given type")
    }

    def stateBranchTotals() {
        println("implement total number and total val of transactions for branches in given state")
    }

    //Customer Details queries
    def customerAccountDetails() {
        prinln("implement check existing account details of a customer")
    }

    def customerUpdateDetails() {
        printn("implement modify existing account details of a customer")
    }

    def customerMonthlyBill() {
        println("implement generate monthly bill for cc number for given month and year")
    }

    def customerTransactionByDateRange() {
        println("implement display transaction made by customer between 2 dates, ordered by year/month/date desc")
    }

    //CC Trans Queries
    def transactionsTopTypes() {
        println("implement plot which transaction types have highest transaction counts")
    }

    def customersTopStateCounts() {
        println("implement plot which states have the highest customer totals")
    }

    def customersTop10Sums() {
        println("implement plot sum of all transaction for top 10 customers and which has highest transaction amount")
    }

    //LOAN Application Queries
    def approvalsSelfEmployed {
        println("implement percentage of applications approved for self-employed applicants")
    }

    def rejectionsMarriedMale {
        println("implemet percentage of applications rejected for married male applicants")
    }

    def transactionsTop3Months {
        println("implement top three months with largest volume of transaction data")
    }

    def transactionsTopDollarHealthcare {
        println("implement branch that processed the highest total dollar value of healthcare transactions")
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
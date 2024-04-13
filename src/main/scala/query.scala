import java.sql.{Connection, DriverManager}
import scala.io.Source
import java.sql.PreparedStatement
import java.sql.SQLException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row


object Queries {
    var connection = getConnection
    var statement = connection.createStatement
    var spark = SparkSession.builder.config("spark.master", "local").getOrCreate()

    var custDF = loadDF("cdw_sapp_customer")
    var creditCardDF = loadDF("cdw_sapp_credit_card")
    var branchDF = loadDF("cdw_sapp_branch")
    var loanDF = loadDF("cdw_sapp_loan_application")


    def testHello {
        println("Hello from the Query File!")
        println("testing access to outerDFS")
        loanDF.show(3)
      
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
    def transactionsByZip(zip: Integer, period: String) {
        println("implement transaction by customers in a given zip for given month and year, ordered by day desc")
    }

    def transactionsByType(transType: String) {
        println("implement total number and total val of transactions by given type")
        var transactionDF = creditCardDF.createOrReplaceTempView("transactions")
        var totalsByTypeDF = spark.sql(f"select count(transaction_id), sum(transaction_value) from transactions where transaction_type = '$transType'")
        totalsByTypeDF.show
    }

    def stateBranchTotals(state: String) {
        println("implement total number and total val of transactions for branches in given state")
        creditCardDF.createOrReplaceTempView("transactions")
    
    }

    //Customer Details queries
    def customerAccountDetails() {
        println("implement check existing account details of a customer")
    }

    def customerUpdateDetails() {
        println("implement modify existing account details of a customer")
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
        var query = "(select TRANSACTION_TYPE, sum(TRANSACTION_VALUE) from cdw_sapp_credit_card group by TRANSACTION_TYPE order by sum(TRANSACTION_VALUE) desc) as highs"
        var df = spark.read
            .format("jdbc")
            .option("driver","com.mysql.cj.jdbc.Driver")
            .option("url","jdbc:mysql://localhost:3306/creditcard_db?autoReconnect=true&useSSL=false")
            .option("dbtable", query)
            .option("user","root")
            .option("password","venusawake")
            .load()

        df.show
    }

    def customersTopStateCounts() {
        println("implement plot which states have the highest customer totals")
        var query = "(select cust_state, count(cust_state) from cdw_sapp_customer group by cust_state order by count(cust_state) desc) as statehighs"
        var df = spark.read
            .format("jdbc")
            .option("driver","com.mysql.cj.jdbc.Driver")
            .option("url","jdbc:mysql://localhost:3306/creditcard_db?autoReconnect=true&useSSL=false")
            .option("dbtable", query)
            .option("user","root")
            .option("password","venusawake")
            .load()

        df.show(60)
    }

    def customersTop10Sums() {    
        println("implement plot sum of all transaction for top 10 customers and which has highest transaction amount")
        
        var transactionDF = creditCardDF.createOrReplaceTempView("transactions")
        var topTenSums = spark.sql("select cust_cc_no, sum(transaction_value) from transactions group by cust_cc_no order by sum(transaction_value) desc limit 10")
        topTenSums.show
        
    }

    //LOAN Application Queries
    def approvalsSelfEmployed {
        println("implement percentage of applications approved for self-employed applicants")
        //var query = "((select count(application_status) from cdw_sapp_loan_application where application_status like 'Yes' and self_employed like 'Yes') / (select count(application_status) from cdw_sapp_loan_application where self_employed like 'Yes')) as approved"
        var df = spark.read
            .format("jdbc")
            .option("driver","com.mysql.cj.jdbc.Driver")
            .option("url","jdbc:mysql://localhost:3306/creditcard_db?autoReconnect=true&useSSL=false")
            .option("dbtable", "cdw_sapp_loan_application")
            .option("user","root")
            .option("password","venusawake")
            .load()
        df.createOrReplaceTempView("loan_application")
        var approvedSE = spark.sql("select count(loan_application.application_status) from loan_application where loan_application.application_status like 'Y' and self_employed like 'Yes'")
        var totalSE = spark.sql("select count(loan_application.application_status) from loan_application where loan_application.self_employed like 'Yes'")
        var t = totalSE.first.getLong(0)
        var a = approvedSE.first.getLong(0)
        println("total apps " + t + " approved: " + a)
        println("percentage of approvals for self-employed applicants: " + (a.toFloat/t*100.00))
        df.show
    }

    def rejectionsMarriedMale {
        println("implemet percentage of applications rejected for married male applicants")
        var df = spark.read
            .format("jdbc")
            .option("driver","com.mysql.cj.jdbc.Driver")
            .option("url","jdbc:mysql://localhost:3306/creditcard_db?autoReconnect=true&useSSL=false")
            .option("dbtable", "cdw_sapp_loan_application")
            .option("user","root")
            .option("password","venusawake")
            .load()
        df.createOrReplaceTempView("loan_application")
        var approvedSE = spark.sql("select count(loan_application.application_status) from loan_application where loan_application.application_status like 'N' and gender like 'Male' and married like 'Yes'")
        var totalSE = spark.sql("select count(loan_application.application_status) from loan_application where loan_application.married like 'Yes' and gender like 'Male'")
        var t = totalSE.first.getLong(0)
        var a = approvedSE.first.getLong(0)
        println("total apps " + t + " approved: " + a)
        println("percentage of approvals for self-employed applicants: " + (a.toFloat/t*100.00))
    }

    def transactionsTop3Months {
        println("implement top three months with largest volume of transaction data")
    }

    def transactionsTopDollarHealthcare {
        println("implement branch that processed the highest total dollar value of healthcare transactions")
         var df = spark.read
            .format("jdbc")
            .option("driver","com.mysql.cj.jdbc.Driver")
            .option("url","jdbc:mysql://localhost:3306/creditcard_db?autoReconnect=true&useSSL=false")
            .option("dbtable", "cdw_sapp_credit_card")
            .option("user","root")
            .option("password","venusawake")
            .load()
        df.createOrReplaceTempView("credit_card")
        var topHealthTranBranchesDF = spark.sql("select branch_code, sum(transaction_value) from credit_card where transaction_type like 'Healthcare' group by branch_code order by sum(transaction_value) desc")
        topHealthTranBranchesDF.show(10)
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

    def purgeDB(): Unit = {
        var statement = connection.createStatement
        var rs = statement.executeUpdate("truncate cdw_sapp_loan_application")
        rs = statement.executeUpdate("truncate cdw_sapp_customer")
        rs = statement.executeUpdate("truncate cdw_sapp_branch")
        rs = statement.executeUpdate("truncate cdw_sapp_credit_card")
        println("all tables truncated")
    }

    def loadDF(tableName: String): Dataset[Row] = {
        var tempDF = spark.read
            .format("jdbc")
            .option("driver","com.mysql.cj.jdbc.Driver")
            .option("url","jdbc:mysql://localhost:3306/creditcard_db?autoReconnect=true&useSSL=false")
            .option("dbtable", tableName)
            .option("user","root")
            .option("password","venusawake")
            .load()
        tempDF = tempDF.toDF
        print(tempDF.getClass)
        tempDF
    }

    
}
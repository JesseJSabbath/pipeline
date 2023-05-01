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

import java.util.Date
import scala.Console.println

object DataLoad {
    def dataLoad: Unit = {
        val spark = SparkSession
            .builder
            .config("spark.master", "local")
            .config("spark.sql.catalogImplementation", "hive")
            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            .enableHiveSupport()
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        import spark.implicits._
        //val apiUrl = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"
        val apiUrl = "https://raw.githubusercontent.com/JesseJSabbath/CourseraWebDevBasics/main/loan_data.json"
        val dbUrl = "jdbc:mysql://localhost:3306/creditcard_db?autoReconnect=true&useSSL=false"
        val driver = "com.mysql.cj.jdbc.Driver"
        val filePath = "/home/jesse/scala/pipeline/.secret.txt"
        val creds = Source.fromFile(filePath).getLines.toArray
        val username = creds(0)
        val password = creds(1)
        
        var connection: Connection = DriverManager.getConnection(dbUrl,username,password)
        var r = requests.get(apiUrl)
        println("Status Code: " + r.statusCode)
        println("Succesfully downloaded data from API")

        var statement = connection.createStatement
        var rs = statement.executeQuery("SELECT * FROM cdw_sapp_customer")
        while(rs.next) {
            print(rs.getString("first_name") + " ")
            println(rs.getString("last_name"))
        }


        //rs = statement.executeUpdate("create table loan_data")

        val jsonStr = r.text
        val df = spark.read.json(Seq(jsonStr).toDS)
        df.show
        df.printSchema
        


        //read and transfomrmation of JSON files below
        val cust_raw = spark.read.json("/home/jesse/scala/pipeline/cdw_sapp_customer.json")
        val branch_raw = spark.read.json("/home/jesse/scala/pipeline/cdw_sapp_branch.json")
        val credit_raw = spark.read.json("/home/jesse/scala/pipeline/cdw_sapp_credit.json")

        cust_raw.printSchema
        branch_raw.printSchema
        credit_raw.printSchema

        
      

        //deleted in lieu of withcolumn concatenationa
        //get 3rd colloumn as array of strings
        //df.rdd.collect.map(row=>row.getString(3))
      //convert original to array of strings in correct format
     // res31.map(num =>  "(" + num.slice(0,3) + ")" + num.slice(3,6) + "-" + num.slice(6,10))
        
        var branchDF = branch_raw.select(col("*"), substring(col("BRANCH_PHONE"), 1,3).as("BP1"))        
            .select(col("*"), substring(col("BRANCH_PHONE"), 4,3).as("BP2"))
            .select(col("*"), substring(col("BRANCH_PHONE"), 7,4).as("BP3"))
            .withColumn("BRANCH_PHONE", concat_ws("",lit("("),col("BP1"),lit(")"),col("BP2"),lit("-"),col("BP3")))
            .withColumn("BZIP", if($"BRANCH_ZIP" == null || $"BRANCH_ZIP" == "") lit("99999") else $"BRANCH_ZIP")
            .drop("BRANCH_ZIP","BP1","BP2","BP3").withColumnRenamed("BZIP","BRANCH_ZIP")//.withColumnRenamed("BRANCH_PHONE_2", "BRANCH_PHONE")

        //conditional on null or "" zipcode:
        //var df6 = df5.withColumn("BZIP", if($"BRANCH_ZIP" == null || $"BRANCH_ZIP" == "") lit("99999") else $"BRANCH_ZIP")
        branchDF.show
        branchDF.write
            .format("jdbc")
            .option("url","jdbc:mysql://localhost:3306/creditcard_db?autoReconnect=true&useSSL=false")
            .option("dbtable", "cdw_sapp_branch")
            .option("user",username)
            .option("password",password)
            .mode("append")
            .save
        branchDF.show

        //construct cdw_sapp_credit_card df w/ TIMEID column for writing
        var creditDF = credit_raw.withColumn("TIMEID", concat_ws("", col("YEAR").cast(StringType), lpad(col("MONTH").cast(StringType), 2, "0"), lpad(col("DAY").cast(StringType),2,"0")))
            .drop("DAY", "MONTH", "YEAR")
            .withColumnRenamed("CREDIT_CARD_NO", "CUST_CC_NO")
        creditDF.write
            .format("jdbc")
            .option("url","jdbc:mysql://localhost:3306/creditcard_db?autoReconnect=true&useSSL=false")
            .option("dbtable", "cdw_sapp_credit_card")
            .option("user",username)
            .option("password",password)
            .mode("append")
            .save
        creditDF.show

        var custDF = cust_raw.withColumn("CUST_PHONE", concat_ws("", lit("(999)"),substring(col("CUST_PHONE"),1,3),lit("-"),substring(col("CUST_PHONE"), 4,4)))
            .withColumn("FULL_STREET_ADDRESS", concat_ws(",", col("STREET_NAME"),col("APT_NO")))
            .withColumn("FIRST_NAME", initcap(col("FIRST_NAME")))
            .withColumn("MIDDLE_NAME", lower(col("MIDDLE_NAME")))
            .withColumn("LAST_NAME",initcap(col("LAST_NAME")))
            .drop("STREET_NAME", "APT_NO")
        custDF.write
            .format("jdbc")
            .option("url","jdbc:mysql://localhost:3306/creditcard_db?autoReconnect=true&useSSL=false")
            .option("dbtable", "cdw_sapp_customer")
            .option("user",username)
            .option("password",password)
            .mode("append")
            .save
        custDF.show
        println(custDF)
    }

    /*def writeTable(tableName: String, df: DataFrame<Row>): Unit = {
        df.write
            .format("jdbc")
            .option("url","jdbc:mysql://localhost:3306/creditcard_db?autoReconnect=true&useSSL=false")
            .option("dbtable", tableName)
            .option("user",username)
            .option("password",password)
            .mode("append")
            .save
    }*/
}



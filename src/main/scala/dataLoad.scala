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
import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.util.Date
import scala.Console.println
import org.apache.log4j.PropertyConfigurator

/*import com.coxautodata.vegalite4s.VegaLite  
import com.coxautodata.vegalite4s.renderers.ImplicitRenderers.AutoSelectionRenderer
import com.coxautodata.vegalite4s.spark.PlotHelpers._
import vegas._ 
import vegas.render.WindowRenderer._ 
import vegas.sparkExt._ 
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._*/


object DataLoad {
    def dataLoad: Unit = {
       

        val spark = SparkSession
            .builder
            .config("spark.master", "local")
            .config("spark.sql.catalogImplementation", "hive")
            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            .enableHiveSupport()
            .getOrCreate()
            
        var log4jConfPath = "/home/jesse/scala/pipeline/src/main/resources/log4j2.properties";

        PropertyConfigurator.configure(log4jConfPath)

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

        //get Loan data file from API (GDrive)
        var r = requests.get(apiUrl)
        println("Succesfully downloaded data from API")
        println("Status Code: " + r.statusCode)
        

        //transform 
        val jsonStr = r.text
        val df = spark.read.json(Seq(jsonStr).toDS)
        println(df.getClass)
      
        df.show
        df.printSchema
        df.write
            .format("jdbc")
            .option("url","jdbc:mysql://localhost:3306/creditcard_db?autoReconnect=true&useSSL=false")
            .option("dbtable", "cdw_sapp_loan_application")
            .option("user",username)
            .option("password",password)
            .mode("append")
            .save

       /*s VegaLite()
            .withObject("""
            {
            "$schema": "https://vega.github.io/schema/vega-lite/v3.json",
            "description": "A simple bar chart with embedded data.",
            "data": {
                "values": [
                {"a": "A","b": 28}, {"a": "B","b": 55}, {"a": "C","b": 43},
                {"a": "D","b": 91}, {"a": "E","b": 81}, {"a": "F","b": 53},
                {"a": "G","b": 19}, {"a": "H","b": 87}, {"a": "I","b": 52}
                ]
            },
            "mark": "bar",
            "encoding": {
                "x": {"field": "a", "type": "ordinal"},
                "y": {"field": "b", "type": "quantitative"}
            }
            }""")
            .show*/
        
            var mfDF = df.groupBy("Gender").agg(count("Gender").as("GenCount"))
            mfDF.show
        
/*        VegaLite()
            .withData(mfDF)
            .withObject("""
            {
                "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
            "description": "A simple bar chart with embedded data.",
            "mark": "line",
            "encoding": {
                "x": {"field": "Gender", "type": "ordinal"},
                "y": {"field": "GenCount", "type": "quantitative"}
            }
            }""")
            .show
            Thread.sleep(25000)
            val plot = Vegas("Loan_Info")
                .withDataFrame(df)
                .encodeX("Gender",Nom)
                .encodeY("GenCount", Quant)
                .mark(Bar)

            plot.window.show 
*/
        //read and transfomrmation of JSON files below
        val cust_raw = spark.read.json("/home/jesse/scala/pipeline/cdw_sapp_customer.json")
        val branch_raw = spark.read.json("/home/jesse/scala/pipeline/cdw_sapp_branch.json")
        val credit_raw = spark.read.json("/home/jesse/scala/pipeline/cdw_sapp_credit.json")

        cust_raw.printSchema
        branch_raw.printSchema
        credit_raw.printSchema

        //transform and write based on mapping document
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

}



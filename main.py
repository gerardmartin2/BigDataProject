import management
from management import *
import analysis
from analysis import *
import runtime
from runtime import *

HADOOP_HOME = "./resources/hadoop_home"
JDBC_JAR = "./resources/postgresql-42.2.8.jar"
PYSPARK_PYTHON = "python3"
PYSPARK_DRIVER_PYTHON = "python3"



if(__name__== "__main__"):
    os.environ["HADOOP_HOME"] = HADOOP_HOME
    sys.path.append(HADOOP_HOME + "\\bin")
    os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
    os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_DRIVER_PYTHON

    spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.jars", "./resources/postgresql-42.2.8.jar") \
    .getOrCreate()

    sc = pyspark.SparkContext.getOrCreate()
    #Load aircraftutilization  from DW
    Aircraft_utilization=(spark.read
        .format("jdbc")
        .option("driver","org.postgresql.Driver")
        .option("url", "jdbc:postgresql://postgresfib.fib.upc.edu:6433/DW?sslmode=require")
        .option("dbtable", "public.aircraftutilization")
        .option("user", "eduard.ramon.aliaga")
        .option("password", "DB240102")
        .load())
    #load operation interruption table from AMOS
    Operation_Interruption=(spark.read
        .format("jdbc")
        .option("driver","org.postgresql.Driver")
        .option("url", "jdbc:postgresql://postgresfib.fib.upc.edu:6433/AMOS?sslmode=require")
        .option("dbtable", "oldinstance.operationinterruption")
        .option("user", "eduard.ramon.aliaga")
        .option("password", "DB240102")
        .load())
    csv_path = "./resources/trainingData/*.csv"
    #Get aircraft and day from user
    print("Give an aircraft:")
    aircraft=str(input())
    print("Give a date (ddmmyy):")
    date=str(input())
    #Delete folders created by the following functions if exixtent
    utils.delete_folders()
    #Pipeline
    data=management.data_management(sc,Aircraft_utilization, Operation_Interruption, aircraft, csv_path)
    analysis.data_analysis(sc,data)
    runtime.data_runtime(sc,Aircraft_utilization, Operation_Interruption, aircraft, csv_path,date)

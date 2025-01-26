from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("HelloWorld-Example") \
    .getOrCreate()

sc = spark.sparkContext

# Read JSON file
try:
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("department", StringType(), True),
        StructField("salary", LongType(), True)
    ])
    df_json = spark.read.schema(schema).option("mode", "PERMISSIVE").json("data/employee-json.json", multiLine=True)
    print("\nJSON File Contents:")
    df_json.show()
except Exception as e:
    print("Error reading JSON file:", e)

# Read CSV file
try:
    df_csv = spark.read.option("header", True).option("mode", "PERMISSIVE").csv("data/employee-csv.csv")
    print("\nCSV File Contents:")
    df_csv.show()
except Exception as e:
    print("Error reading CSV file:", e)

# Read Text file
try:
    rdd_text = sc.textFile("data/employee-text.txt")
    print("\nTXT File Contents:")
    print(rdd_text.take(5))
    print("\nforeach() Output:")
    rdd_text.foreach(print)
except Exception as e:
    print("Error reading text file:", e)

# Stop Spark session
spark.stop()

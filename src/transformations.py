from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.functions import col, avg, count, when

spark = SparkSession.builder \
    .appName("Transformations-Example") \
    .getOrCreate()

sc = spark.sparkContext

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("department", StringType(), True),
    StructField("salary", LongType(), True)
])

try:
    # 1. Read the data
    df = spark.read.schema(schema).json("data/employee-json.json", multiLine=True)
    df.show()
    
    # 2. Filter employees age > 30 
    df_transformed = df.filter(df.age >= 30)
    df_transformed.show()
    
    # 3. Fill missing salary with avg salary
    # Calculate the average salary by department
    avg_salary_df = df.groupBy("department").agg(avg("salary").alias("avg_salary"))
    avg_salary_df.show()

    # Join the average salary with the original DataFrame
    df_with_avg = df.join(avg_salary_df, on="department", how="left")
    df_with_avg.show()
    
    # Fill missing salary with the department's average salary
    df_transformed = df_with_avg.withColumn("salary", \
        when(col("avg_salary").isNull(), 0) \
        .when(col("salary").isNull() | (col("salary") == 0), col("avg_salary")) \
        .otherwise(col("salary"))
    ).select("id", "name", "department", "salary")
    df_transformed.show()
    
except Exception as e:
    print("Error reading JSON file.", e)
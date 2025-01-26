from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Functions-Example") \
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
    print("\nSchema:", df_json.schema)
    print("\nColumns:", df_json.columns)
    print("\nDescribe:", df_json.describe().show())
    print("\nCount:", df_json.count())
    print("\nFirst Row:", df_json.first())
    print("\nFirst 3 Rows:", df_json.head(3))
    print("\nSelect Column:", df_json.select("name").show())
    print("\nFilter Data:", df_json.filter(df_json.age > 30).show())
    print("\nGroup By Data:", df_json.groupBy("department").count().show())
    print("\nOrder By Data:", df_json.orderBy(df_json.salary.desc()).show())
    print("\nDistinct Data:", df_json.select("department").distinct().show())
    print("\nDrop Column:", df_json.drop("department").show())
    print("\nDrop Duplicates:", df_json.dropDuplicates().show())
    print("\nDrop NA:", df_json.dropna().show())
    print("\nFill NA:", df_json.fillna("Unknown").show())
    print("\nReplace Data:", df_json.replace("IT", "Information Technology").show())
    print("\nCollect Data:", df_json.collect())
    print("\nTake Data:", df_json.take(2))
    
    ## RDD Operations
    print("\nMap Data:", df_json.rdd.map(lambda x: (x.name, x.age)).collect())
    print("\nFlatMap Data:", df_json.rdd.flatMap(lambda x: (x.name, x.age)).collect())
    print("\nReduce Data:", df_json.rdd.map(lambda x: x.salary).reduce(lambda x, y: x + y))
    print("\nAggregate Data:", df_json.rdd.map(lambda x: x.salary).aggregate((0, 0), (lambda x, y: (x[0] + y, x[1] + 1))))
    print("\nCount By Value:", df_json.rdd.map(lambda x: x.department).countByValue())
    print("\nGroup By Key:", df_json.rdd.map(lambda x: (x.department, x.salary)).groupByKey().mapValues(list).collect())
    print("\nReduce By Key:", df_json.rdd.map(lambda x: (x.department, x.salary)).reduceByKey(lambda x, y: x + y).collect())
    print("\nJoin Data:", df_json.rdd.map(lambda x: (x.id, x.name)).join(df_json.rdd.map(lambda x: (x.id, x.department))).collect())
    print("\nLeft Join Data:", df_json.rdd.map(lambda x: (x.id, x.name)).leftOuterJoin(df_json.rdd.map(lambda x: (x.id, x.department))).collect())
    print("\nRight Join Data:", df_json.rdd.map(lambda x: (x.id, x.name)).rightOuterJoin(df_json.rdd.map(lambda x: (x.id, x.department))).collect())
    print("\nFull Join Data:", df_json.rdd.map(lambda x: (x.id, x.name)).fullOuterJoin(df_json.rdd.map(lambda x: (x.id, x.department))).collect())
    print("\nUnion Data:", df_json.rdd.map(lambda x: (x.id, x.name)).union(df_json.rdd.map(lambda x: (x.id, x.department))).collect())
    print("\nIntersection Data:", df_json.rdd.map(lambda x: (x.id, x.name)).intersection(df_json.rdd.map(lambda x: (x.id, x.department))).collect())
    print("\nSubtract Data:", df_json.rdd.map(lambda x: (x.id, x.name)).subtract(df_json.rdd.map(lambda x: (x.id, x.department))).collect())
    print("\nCartesian Data:", df_json.rdd.map(lambda x: (x.id, x.name)).cartesian(df_json.rdd.map(lambda x: (x.id, x.department))).collect())
    print("\nZip Data:", df_json.rdd.map(lambda x: x.id).zip(df_json.rdd.map(lambda x: x.name)).collect())
    print("\nKey By Data:", df_json.rdd.keyBy(lambda x: x.id).collect())
    print("\nPartition By Data:", df_json.rdd.map(lambda x: (x.id, x.name)).partitionBy(2).glom().collect())
    print("\nCoalesce Data:", df_json.rdd.map(lambda x: (x.id, x.name)).coalesce(1).glom().collect())
    print("\nRepartition Data:", df_json.rdd.map(lambda x: (x.id, x.name)).repartition(2).glom().collect())
    print("\nCoalesce Data:", df_json.rdd.map(lambda x: (x.id, x.name)).coalesce(1).glom().collect())
    print("\nRepartition Data:", df_json.rdd.map(lambda x: (x.id, x.name)).repartition(2).glom().collect())
    print("\nPersist Data:", df_json.rdd.map(lambda x: (x.id, x.name)).persist().glom().collect())
    print("\nUn-persist Data:", df_json.rdd.map(lambda x: (x.id, x.name)).unpersist().glom().collect())
    print("\nCollect As Map:", df_json.rdd.map(lambda x: (x.id, x.name)).collectAsMap())
    print("\nCount By Key:", df_json.rdd.map(lambda x: (x.id, x.name)).countByKey())
    print("\nLookup Data:", df_json.rdd.map(lambda x: (x.id, x.name)).lookup(1))
    print("\nTake Ordered Data:", df_json.rdd.map(lambda x: (x.id, x.name)).takeOrdered(2))
    print("\nTop Data:", df_json.rdd.map(lambda x: (x.id, x.name)).top(2))
    print("\nTake Sample Data:", df_json.rdd.map(lambda x: (x.id, x.name)).takeSample(False, 2))
    print("\nRandom Split Data:", df_json.rdd.map(lambda x: (x.id, x.name)).randomSplit([0.5, 0.5]))
    print("\nFilter Data:", df_json.rdd.filter(lambda x: x.age > 30).collect())
    print("\nMap Partitions Data:", df_json.rdd.mapPartitions(lambda x: [y for y in x if y.age > 30]).collect())
    print("\nMap Partitions With Index Data:", df_json.rdd.mapPartitionsWithIndex(lambda index, x: [y for y in x if y.age > 30]).collect())
    print("\nPrint Schema:")
    df_json.printSchema()
except Exception as e:
    print("Error reading JSON file:", e)

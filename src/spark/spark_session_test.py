from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("Week6-Banking-Spark-Test")
    .master("local[*]")
    .getOrCreate()
)

print("SparkSession created successfully!")
print("Spark version:", spark.version)


data = [
    (1, "T1001", "CREDIT", 550.0),
    (2, "T1002", "DEBIT", 100.0),
    (3, "T1003", "CREDIT", 800.0),
]

columns = [
    "id",
    "transaction_id",
    "transaction_type",
    "amount",
]


df = spark.createDataFrame(data, columns)

print("\nDataFrame:")
df.show()

print("\nSchema:")
df.printSchema()

print("\nRow count:", df.count())


spark.stop()
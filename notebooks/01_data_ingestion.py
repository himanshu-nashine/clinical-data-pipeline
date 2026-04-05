from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ClinicalPipeline").getOrCreate()

dm = spark.read.csv("data/raw/dm.csv", header=True, inferSchema=True)
ae = spark.read.csv("data/raw/ae.csv", header=True, inferSchema=True)
lb = spark.read.csv("data/raw/lb.csv", header=True, inferSchema=True)

dm.show()

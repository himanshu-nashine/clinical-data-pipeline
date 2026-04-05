from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ClinicalPipeline").getOrCreate()

df = spark.read.csv("data/processed/adam_dataset.csv", header=True, inferSchema=True)

if df.filter(df.SUBJID.isNull()).count() == 0:
    print("Validation Passed")
else:
    print("Validation Failed")

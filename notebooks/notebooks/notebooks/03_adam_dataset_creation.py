from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ClinicalPipeline").getOrCreate()

df = spark.read.csv("data/raw/dm.csv", header=True, inferSchema=True)

adam_df = df.select("SUBJID", "AGE", "SEX", "ARM")

adam_df.write.csv("data/processed/adam_dataset.csv", header=True)

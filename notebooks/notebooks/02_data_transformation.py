from pyspark.sql.functions import col
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ClinicalPipeline").getOrCreate()

dm = spark.read.csv("data/raw/dm.csv", header=True, inferSchema=True)
ae = spark.read.csv("data/raw/ae.csv", header=True, inferSchema=True)
lb = spark.read.csv("data/raw/lb.csv", header=True, inferSchema=True)

clinical_df = dm.join(ae, "SUBJID", "left") \
                .join(lb, "SUBJID", "left")

clinical_df = clinical_df.withColumn(
    "AE_FLAG",
    col("AESER") == "Y"
)

clinical_df.show()

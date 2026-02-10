from pyspark.sql import SparkSession, DataFrame

def spark_read_industry(spark: SparkSession, bucket: str) -> DataFrame:
    industries = spark.read \
        .option("inferSchema", True) \
        .option('header', True) \
        .option('sep', ',') \
        .csv(f's3a://{bucket}/industries/industries_list.csv')

    return industries
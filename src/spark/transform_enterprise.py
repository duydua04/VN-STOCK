from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date

def get_full_enterprise_infomation(spark: SparkSession, bucket: str) -> DataFrame:
    basic_enterprise = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .option("sep", ",") \
        .csv(f's3a://{bucket}/enterprise/enterprise_basic_info.csv')

    industry_by_tax_code = spark.read \
        .option("header", True) \
        .option('inferSchema', True) \
        .option("sep", ",") \
        .csv("../../data/vn50_industry_final.csv")

    full_enterprise = basic_enterprise \
        .withColumn('listing_date', to_date(col('listing_date'), 'dd/MM/yyyy')) \
        .join(industry_by_tax_code, ['code', 'tax_code'], 'inner') \
        .withColumnRenamed('code', 'stock_code')


    return full_enterprise


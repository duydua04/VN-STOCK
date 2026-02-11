from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

def transform_subcompany(spark: SparkSession, bucket: str):
    sub_comp = spark.read \
        .option("inferSchema", True) \
        .option('header', True) \
        .option('sep', ',') \
        .csv(f's3a://{bucket}/subsidiaries/subs_comp.csv')

    names_map = {
        'stock_code': 'parent_stock_code',
        'comp_name': 'subsidiary_name',
        'comp_type': 'subsidiary_type'

    }

    sub_comp = sub_comp \
        .withColumnsRenamed(names_map) \
        .withColumn('subsidiary_type',
                    when(col('subsidiary_type') == 'aff', lit('Affiliate'))
                    .otherwise(lit('Subsidiary'))
        )

    return sub_comp
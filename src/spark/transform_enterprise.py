from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, LongType, DecimalType


def get_full_enterprise_information(spark: SparkSession, bucket: str) -> DataFrame:
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
        .withColumn('listing_date', F.to_date(F.col('listing_date'), 'dd/MM/yyyy')) \
        .join(industry_by_tax_code, ['code', 'tax_code'], 'inner') \
        .withColumnRenamed('code', 'stock_code')


    return full_enterprise

def read_leader_ship(spark: SparkSession, bucket: str) -> DataFrame:
    leader_ship = spark.read \
        .option('header', True) \
        .option('inferSchema', True) \
        .option('sep', ',') \
        .csv(f's3a://{bucket}/profile/leadership.csv')

    return leader_ship


def transform_leadership(df_leader_ship):
    df_transformed = df_leader_ship.withColumn(
        "holder_uid",
        F.concat(F.col("stock_code"), F.lit("_"), F.col("id"))
    )

    is_org_logic = F.lit(False)

    df_dim_holder = df_transformed.select(
        F.col("holder_uid").alias("holder_id"),
        F.col("officer_name").alias("full_name"),
        is_org_logic.cast(BooleanType()).alias("is_organization"),
        F.col("update_date").cast(DateType()).alias("updated_at")
    ).dropDuplicates(["holder_id"])

    df_fact_ownership = df_transformed.select(
        F.col("stock_code"),
        F.col("holder_uid").alias("holder_id"),
        F.col("officer_position").alias("position"),
        F.col("quantity").cast(LongType()).alias("shares_count"),

        F.col("officer_own_percent").cast(DecimalType(10, 6)).alias("ownership_ratio"),

        F.col("update_date").cast(DateType()).alias("report_date")
    )

    return df_dim_holder, df_fact_ownership
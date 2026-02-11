from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, LongType, DateType
from spark_connect import sc

def _remove_empty_columns(df) -> DataFrame:
    counts = df.agg(*[F.count(c).alias(c) for c in df.columns]).collect()[0]
    to_drop = [c for c in df.columns if counts[c] == 0]
    if to_drop:
        df = df.drop(*to_drop)

    return df

def read_income_data(spark: SparkSession, bucket: str) -> DataFrame:
    raw_income = spark.read \
        .option('header', True) \
        .option('inferSchema', True) \
        .option('sep', ',') \
        .csv(f's3a://{bucket}/financial/income/income_data.csv')

    # Xóa cột rỗng trước
    raw_income = _remove_empty_columns(raw_income)

    if "ticker" in raw_income.columns:
        raw_income = raw_income.withColumnRenamed("ticker", "stock_code")
        if "CP" in raw_income.columns:
            raw_income = raw_income.drop("CP")
    elif "CP" in raw_income.columns:
        raw_income = raw_income.withColumnRenamed("CP", "stock_code")

    if "Năm" in raw_income.columns:
        raw_income = raw_income.withColumnRenamed("Năm", "year")
    if "Kỳ" in raw_income.columns:
        raw_income = raw_income.withColumnRenamed("Kỳ", "quarter")

    raw_income = raw_income.withColumn("year", F.col("year").cast(IntegerType())) \
        .withColumn("quarter", F.col("quarter").cast(IntegerType()))

    return raw_income


def read_balance_data(spark: SparkSession, bucket: str) -> DataFrame:
    raw_balance = spark.read \
        .option('header', True) \
        .option('inferSchema', True) \
        .option('sep', ',') \
        .csv(f's3a://{bucket}/financial/balance/balance_data.csv')

    raw_balance = _remove_empty_columns(raw_balance)

    if "ticker" in raw_balance.columns:
        raw_balance = raw_balance.withColumnRenamed("ticker", "stock_code")
        if "CP" in raw_balance.columns:
            raw_balance = raw_balance.drop("CP")
    elif "CP" in raw_balance.columns:
        raw_balance = raw_balance.withColumnRenamed("CP", "stock_code")

    if "Năm" in raw_balance.columns:
        raw_balance = raw_balance.withColumnRenamed("Năm", "year")
    if "Kỳ" in raw_balance.columns:
        raw_balance = raw_balance.withColumnRenamed("Kỳ", "quarter")

    raw_balance = raw_balance.withColumn("year", F.col("year").cast(IntegerType())) \
        .withColumn("quarter", F.col("quarter").cast(IntegerType()))

    return raw_balance


def read_price_data(spark: SparkSession, bucket: str) -> DataFrame:
    df = spark.read \
        .option('header', True) \
        .option('inferSchema', True) \
        .option('sep', ',') \
        .csv(f's3a://{bucket}/market/prices/stock_price.csv')

    if "ticker" in df.columns:
        df = df.withColumnRenamed("ticker", "stock_code")

    df_clean = df.select(
        F.col("stock_code"),
        F.col("trade_date").cast(DateType()),
        F.col("open").cast(DecimalType(15, 2)),
        F.col("high").cast(DecimalType(15, 2)),
        F.col("low").cast(DecimalType(15, 2)),
        F.col("close").cast(DecimalType(15, 2)),
        F.col("volume").cast(LongType())
    )

    df_final = df_clean.withColumn("year", F.year("trade_date")) \
        .withColumn("quarter", F.quarter("trade_date"))

    return df_final


def transform_fact_price_history(df_price_raw, df_balance_raw):
    df_shares = calculate_outstanding_shares(df_balance_raw)

    df_price = df_price_raw.withColumnRenamed("ticker", "stock_code") \
        .withColumn("trade_date", F.col("trade_date").cast(DateType())) \
        .withColumn("year", F.year("trade_date")) \
        .withColumn("quarter", F.quarter("trade_date"))

    if df_shares is not None:
        df_joined = df_price.join(df_shares, on=["stock_code", "year", "quarter"], how="left")

        df_final_calc = df_joined.withColumn(
            "market_cap",
            (F.col("close") * F.coalesce(F.col("outstanding_shares"), F.lit(0))).cast(DecimalType(20, 2))
        )
    else:
        df_final_calc = df_price.withColumn("market_cap", F.lit(None).cast(DecimalType(20, 2)))

    df_final = df_final_calc.select(
        F.col("stock_code"),
        F.col("trade_date"),
        F.col("open").cast(DecimalType(15, 2)),
        F.col("high").cast(DecimalType(15, 2)),
        F.col("low").cast(DecimalType(15, 2)),
        F.col("close").cast(DecimalType(15, 2)),
        F.col("volume").cast("long"),
        F.col("market_cap")
    )

    return df_final

def process_financial_reports(df_income: DataFrame, df_balance: DataFrame):
    def get_col(columns_list, col_name, default_val=None):
        if col_name in columns_list:
            return F.col(col_name)
        return F.lit(default_val)

    inc_cols = df_income.columns

    revenue_logic = F.coalesce(
        get_col(inc_cols, "Doanh thu thuần"),
        get_col(inc_cols, "Doanh thu (đồng)"),
        (F.coalesce(get_col(inc_cols, "Thu nhập lãi thuần", 0), F.lit(0)) +
         F.coalesce(get_col(inc_cols, "Lãi thuần từ hoạt động dịch vụ", 0), F.lit(0)))
    )

    gross_profit_logic = F.coalesce(
        get_col(inc_cols, "Lãi gộp"),
        get_col(inc_cols, "Doanh thu thuần", 0) + get_col(inc_cols, "Giá vốn hàng bán", 0)
    )

    net_profit_logic = F.coalesce(
        get_col(inc_cols, "Lợi nhuận sau thuế của Cổ đông công ty mẹ (đồng)"),
        get_col(inc_cols, "Lợi nhuận sau thuế của Cổ đông công ty mẹ"),
        get_col(inc_cols, "Cổ đông của Công ty mẹ"),
        get_col(inc_cols, "Lợi nhuận sau thuế"),
        F.lit(0)
    )

    df_income_mapped = df_income.select(
        F.col("stock_code"),
        F.col("year"),
        F.col("quarter"),
        F.lit("IncomeStatement").alias("report_type"),

        revenue_logic.cast(DecimalType(20, 2)).alias("revenue"),
        gross_profit_logic.cast(DecimalType(20, 2)).alias("gross_profit"),

        F.coalesce(
            get_col(inc_cols, "Lãi/Lỗ từ hoạt động kinh doanh"),
            get_col(inc_cols, "Lợi nhuận thuần")
        ).cast(DecimalType(20, 2)).alias("operating_profit"),

        net_profit_logic.cast(DecimalType(20, 2)).alias("net_profit"),

        F.lit(None).cast(DecimalType(20, 2)).alias("total_assets"),
        F.lit(None).cast(DecimalType(20, 2)).alias("total_equity"),
        F.lit(None).cast(DecimalType(20, 2)).alias("total_liabilities"),
        F.lit(None).cast(DecimalType(20, 2)).alias("short_term_debt"),
        F.lit(None).cast(DecimalType(20, 2)).alias("long_term_debt")
    )

    bal_cols = df_balance.columns

    df_balance_mapped = df_balance.select(
        F.col("stock_code"),
        F.col("year"),
        F.col("quarter"),
        F.lit("BalanceSheet").alias("report_type"),

        F.lit(None).cast(DecimalType(20, 2)).alias("revenue"),
        F.lit(None).cast(DecimalType(20, 2)).alias("gross_profit"),
        F.lit(None).cast(DecimalType(20, 2)).alias("operating_profit"),
        F.lit(None).cast(DecimalType(20, 2)).alias("net_profit"),

        get_col(bal_cols, "TỔNG CỘNG TÀI SẢN (đồng)").cast(DecimalType(20, 2)).alias("total_assets"),
        get_col(bal_cols, "VỐN CHỦ SỞ HỮU (đồng)").cast(DecimalType(20, 2)).alias("total_equity"),
        get_col(bal_cols, "NỢ PHẢI TRẢ (đồng)").cast(DecimalType(20, 2)).alias("total_liabilities"),

        get_col(bal_cols, "Vay và nợ thuê tài chính ngắn hạn (đồng)").cast(DecimalType(20, 2)).alias("short_term_debt"),
        get_col(bal_cols, "Vay và nợ thuê tài chính dài hạn (đồng)").cast(DecimalType(20, 2)).alias("long_term_debt")
    )

    df_final = df_income_mapped.unionByName(df_balance_mapped)

    return df_final


def calculate_outstanding_shares(df_balance) -> DataFrame:
    df_shares = df_balance.select(
        F.col("stock_code"),
        F.col("year"),
        F.col("quarter"),
        (F.col("Vốn góp của chủ sở hữu (đồng)") / 10000).cast(LongType()).alias("outstanding_shares")
    )

    return df_shares


def transform_financial_ratio(df_financial_report, df_price_history):
    w_last_day = Window.partitionBy("stock_code", "year", "quarter").orderBy(F.col("trade_date").desc())

    df_price_quarterly = df_price_history.withColumn("rn", F.row_number().over(w_last_day)) \
        .filter(F.col("rn") == 1) \
        .select(
        F.col("stock_code"),
        F.col("year"),
        F.col("quarter"),
        F.col("close").alias("close_price"),
        F.col("market_cap")
    )

    df_joined = df_financial_report.join(
        df_price_quarterly,
        on=["stock_code", "year", "quarter"],
        how="left"
    )

    def safe_div(num, denom):
        return F.when((denom == 0) | (denom.isNull()), None).otherwise(num / denom)

    shares_col = safe_div(F.col("market_cap"), F.col("close_price"))

    df_calc = df_joined.select(
        F.col("stock_code"),
        F.col("year"),
        F.col("quarter"),
        F.concat(F.lit("Q"), F.col("quarter"), F.lit("/"), F.col("year")).alias("report_period"),
        safe_div(F.col("net_profit"), F.col("revenue")).cast(DecimalType(10, 4)).alias("net_margin"),
        safe_div(F.col("net_profit"), F.col("total_equity")).cast(DecimalType(10, 4)).alias("roe"),
        safe_div(F.col("net_profit"), F.col("total_assets")).cast(DecimalType(10, 4)).alias("roa"),
        safe_div(F.col("net_profit"), shares_col).cast(DecimalType(15, 2)).alias("eps"),
        safe_div(F.col("total_equity"), shares_col).cast(DecimalType(15, 2)).alias("book_value"),
        safe_div(F.col("close_price"), safe_div(F.col("net_profit"), shares_col)).cast(DecimalType(10, 2)).alias("pe"),
        safe_div(F.col("market_cap"), F.col("total_equity")).cast(DecimalType(10, 2)).alias("pb")
    )

    return df_calc


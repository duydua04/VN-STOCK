import time
import os
import sys
import pandas as pd
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.crawl.stock_list import get_vn50_list
from src.crawl.enterprise_crawler import (
    get_industries_master_data,
    get_all_enterprise_information,
    get_sub_companies
)
from src.crawl.profile_crawler import VnstockProfileCrawler
from src.crawl.financial_crawler import VnstockDataCrawler
from src.utils.bucket_manager import BucketManager
from configs.db_config import get_database_config
from src.sql.init_schema import init_database_schema

# --- XÓA TOÀN BỘ IMPORT LIÊN QUAN ĐẾN THỰC THI SPARK Ở ĐÂY ---
from src.spark.spark_industry import spark_read_industry
from src.spark.transform_enterprise import get_full_enterprise_information, read_leader_ship, transform_leadership
from src.spark.transform_subcom import transform_subcompany
from src.spark.transform_financial import (
    read_income_data, read_balance_data, read_price_data,
    process_financial_reports, transform_fact_price_history,
    transform_financial_ratio, transform_dim_date
)

BUCKET_NAME = "vn-stock-data-lake"
bm = BucketManager()
db_config = get_database_config()

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='vn-stock-pipeline-full',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['vn50', 'end-to-end', 'spark-postgres']
)
def stock_full_etl():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    @task
    def task_init_db():
        """Tao database va init schema"""
        print("Initializing Database Schema...")
        init_database_schema(db_config)

    @task
    def get_list_tickers():
        df = get_vn50_list()
        return df['stock_code'].tolist()

    @task
    def crawl_master_industry():
        industries = get_industries_master_data()
        bm.save_dataframe(df=industries, bucket=BUCKET_NAME, folder="industries",
                          file_name="industries_list", file_format="csv")

    @task
    def crawl_basic_info(tickers):
        df_input = pd.DataFrame({'code': tickers})
        enterprise_info = get_all_enterprise_information(df_input)
        bm.save_dataframe(df=enterprise_info, bucket=BUCKET_NAME, folder="enterprise",
                          file_name='enterprise_basic_info', file_format='csv')

    @task
    def crawl_subsidiaries(tickers):
        df_input = pd.DataFrame({'code': tickers})
        sub_comp = get_sub_companies(df_input)
        bm.save_dataframe(df=sub_comp, bucket=BUCKET_NAME, folder="subsidiaries",
                          file_name="subs_comp", file_format='csv')

    @task
    def crawl_leadership(tickers):
        crawler = VnstockProfileCrawler()
        all_data = [crawler.fetch_officers(s) for s in tickers]
        leaderships = pd.concat([d for d in all_data if d is not None])
        bm.save_dataframe(df=leaderships, bucket=BUCKET_NAME, folder="profile",
                          file_name="leadership", file_format='csv')

    @task
    def crawl_financials(tickers):
        crawler = VnstockDataCrawler(max_workers=1)

        print("--- Crawling Income Statement ---")
        income = crawler.fetch_financial_reports(tickers, 'income_statement')
        bm.save_dataframe(df=income, bucket=BUCKET_NAME, folder="financial/income",
                          file_name="income_data", file_format='csv')

        print("Sleeping 65s to reset API Limit...")
        time.sleep(65)

        print("--- Crawling Balance Sheet ---")
        balance = crawler.fetch_financial_reports(tickers, 'balance_sheet')
        if balance is None or balance.empty:
            print("Balance Sheet trả về rỗng! Kiểm tra lại API Limit.")

        bm.save_dataframe(df=balance, bucket=BUCKET_NAME, folder="financial/balance",
                          file_name="balance_data", file_format='csv')

        print("Sleeping 65s to reset API Limit...")
        time.sleep(65)

        print("--- Crawling Cash Flow ---")
        cash_flow = crawler.fetch_financial_reports(tickers, 'cash_flow')
        bm.save_dataframe(df=cash_flow, bucket=BUCKET_NAME, folder="financial/cashflow",
                          file_name="cashflow_data", file_format='csv')

    @task
    def crawl_stock_price(tickers):
        stock_price = VnstockDataCrawler(max_workers=2).fetch_price_history(tickers, '2024-01-01', '2025-12-31')
        bm.save_dataframe(df=stock_price, bucket=BUCKET_NAME, folder="market/prices", file_name="stock_price",
                          file_format='csv')

    @task
    def process_industry():
        from src.spark.spark_connect import create_spark_connect
        from src.spark.spark_write_postgres import SparkWritePostgres
        
        sc_instance = create_spark_connect()
        spark = sc_instance.spark
        writer = SparkWritePostgres(spark, db_config)
        
        try:
            df_ind = spark_read_industry(spark, BUCKET_NAME)
            writer.write_data(df_ind, "dim_industry", mode="append")
        finally:
            spark.stop()

    @task
    def process_enterprise():
        from src.spark.spark_connect import create_spark_connect
        from src.spark.spark_write_postgres import SparkWritePostgres
        
        sc_instance = create_spark_connect()
        spark = sc_instance.spark
        writer = SparkWritePostgres(spark, db_config)
        
        try:
            df_ent = get_full_enterprise_information(spark, BUCKET_NAME)
            writer.write_data(df_ent, "dim_enterprise", mode="append")
        finally:
            spark.stop()

    @task
    def process_subsidiaries():
        from src.spark.spark_connect import create_spark_connect
        from src.spark.spark_write_postgres import SparkWritePostgres
        
        sc_instance = create_spark_connect()
        spark = sc_instance.spark
        writer = SparkWritePostgres(spark, db_config)
        
        try:
            df_sub = transform_subcompany(spark, BUCKET_NAME)
            writer.write_data(df_sub, "fact_subsidiary", mode="append")
        finally:
            spark.stop()

    @task
    def process_leadership_ownership():
        from src.spark.spark_connect import create_spark_connect
        from src.spark.spark_write_postgres import SparkWritePostgres
        
        sc_instance = create_spark_connect()
        spark = sc_instance.spark
        writer = SparkWritePostgres(spark, db_config)
        
        try:
            df_raw = read_leader_ship(spark, BUCKET_NAME)
            df_dim_holder, df_fact_own = transform_leadership(df_raw)

            writer.write_data(df_dim_holder, "dim_holder_info", mode="append")
            writer.write_data(df_fact_own, "fact_ownership", mode="append")
        finally:
            spark.stop()

    @task
    def process_financials_and_prices():
        from src.spark.spark_connect import create_spark_connect
        from src.spark.spark_write_postgres import SparkWritePostgres
        
        sc_instance = create_spark_connect()
        spark = sc_instance.spark
        writer = SparkWritePostgres(spark, db_config)
        
        try:
            df_income = read_income_data(spark, BUCKET_NAME)
            df_balance = read_balance_data(spark, BUCKET_NAME)
            df_price_raw = read_price_data(spark, BUCKET_NAME)

            df_fin_report = process_financial_reports(df_income, df_balance)
            writer.write_data(df_fin_report, "fact_financial_report", mode="overwrite")

            df_fact_price = transform_fact_price_history(df_price_raw, df_balance)
            writer.write_data(df_fact_price, "fact_price_history", mode="overwrite")

            df_dim_date = transform_dim_date(df_price_raw)
            writer.write_data(df_dim_date, "dim_date", mode="overwrite")

            df_ratios = transform_financial_ratio(df_fin_report, df_fact_price)
            writer.write_data(df_ratios, "fact_financial_ratio", mode="overwrite")
        finally:
            spark.stop()

    init_db_task = task_init_db()

    tickers = get_list_tickers()
    crawl_ind_task = crawl_master_industry()

    t_basic = crawl_basic_info(tickers)
    t_sub = crawl_subsidiaries(tickers)
    t_lead = crawl_leadership(tickers)
    t_fin = crawl_financials(tickers)
    t_price = crawl_stock_price(tickers)

    crawl_tasks = [t_basic, t_sub, t_lead, t_fin, t_price]

    p_ind = process_industry()
    p_ent = process_enterprise()
    p_sub = process_subsidiaries()
    p_lead = process_leadership_ownership()
    p_fin_price = process_financials_and_prices()

    start >> init_db_task >> [tickers, crawl_ind_task]

    crawl_ind_task >> p_ind
    tickers >> crawl_tasks

    [t_basic, p_ind] >> p_ent

    p_ent >> [p_sub, p_lead, p_fin_price]

    t_sub >> p_sub
    t_lead >> p_lead
    [t_fin, t_price] >> p_fin_price

    [p_sub, p_lead, p_fin_price] >> end

dag_obj = stock_full_etl()
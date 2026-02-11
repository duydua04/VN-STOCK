from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
import os, sys
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

# --- CONFIG ---
BUCKET_NAME = "test-stock-data-lake"
bm = BucketManager()

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='vn-stock-pipeline1',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['vn50', 'raw_layer', 'full_crawl']
)
def stock_full_etl():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # LẤY DANH SÁCH VN50 ---
    @task
    def get_list_tickers():
        df = get_vn50_list()
        return df['stock_code'].tolist()

    @task
    def crawl_master_industry():
        """Lấy danh mục mã ngành"""
        industries = get_industries_master_data()
        bm.save_dataframe(
            df=industries,
            bucket=BUCKET_NAME,
            folder="industries",
            file_name="industries_list",
            file_format="csv"
        )

    @task
    def crawl_basic_info(tickers):
        """Lấy danh sách thông tin từ Vietstock"""
        df_input = pd.DataFrame({'code': tickers})
        enterprise_info = get_all_enterprise_information(df_input)
        bm.save_dataframe(
            df=enterprise_info,
            bucket=BUCKET_NAME,
            folder="enterprise",
            file_name='enterprise_basic_info',
            file_format='csv'
        )
        # Trả về data để task tra cứu ngành theo MST sử dụng (Data Lineage)
        return enterprise_info.to_dict('records')


    @task
    def crawl_subsidiaries(tickers):
        """Lấy danh sách công ty con/liên kết từ CafeF"""
        df_input = pd.DataFrame({'code': tickers})
        sub_comp = get_sub_companies(df_input)
        bm.save_dataframe(
            df=sub_comp,
            bucket=BUCKET_NAME,
            folder="subsidiaries",
            file_name="subs_comp",
            file_format='csv'
        )

    @task
    def crawl_leadership(tickers):
        crawler = VnstockProfileCrawler()
        all_data = [crawler.fetch_officers(s) for s in tickers]
        leaderships = pd.concat([d for d in all_data if d is not None])
        bm.save_dataframe(
            df=leaderships,
            bucket=BUCKET_NAME,
            folder="profile",
            file_name="leadership",
            file_format='csv'
        )

    @task
    def crawl_inc_state(tickers):
        income = VnstockDataCrawler(max_workers=2) \
              .fetch_financial_reports(tickers, 'income_statement')

        bm.save_dataframe(
            df=income,
            bucket=BUCKET_NAME,
            folder="financial/income",
            file_name="income_data",
            file_format='csv'
        )

    @task
    def crawl_balance_sheet(tickers):
        balance = VnstockDataCrawler(max_workers=2) \
              .fetch_financial_reports(tickers, 'balance_sheet')

        bm.save_dataframe(
            df=balance,
            bucket=BUCKET_NAME,
            folder="financial/balance",
            file_name="balance_data",
            file_format='csv'
        )

    @task
    def crawl_cash_flow(tickers):
        cash_flow = VnstockDataCrawler(max_workers=2) \
            .fetch_financial_reports(tickers, 'cash_flow')

        bm.save_dataframe(
            df=cash_flow,
            bucket=BUCKET_NAME,
            folder="financial/cashflow",
            file_name="cashflow_data",
            file_format='csv'
        )

    @task
    def crawl_stock_price(tickers):
        stock_price = VnstockDataCrawler(max_workers=2) \
            .fetch_price_history(tickers, '2025-01-01', '2025-12-31')

        bm.save_dataframe(
            df=stock_price,
            bucket=BUCKET_NAME,
            folder="market/prices",
            file_name="stock_price",
            file_format='csv'
        )

    tickers_list = get_list_tickers()
    crawl_industry = crawl_master_industry()

    # Các task phụ thuộc vào tickers_list
    task_basic_info = crawl_basic_info(tickers_list)
    task_subcomp = crawl_subsidiaries(tickers_list)
    task_lead = crawl_leadership(tickers_list)
    task_inc = crawl_inc_state(tickers_list)
    task_bal = crawl_balance_sheet(tickers_list)
    task_cf = crawl_cash_flow(tickers_list)
    task_price = crawl_stock_price(tickers_list)

    # Gom nhóm các task chạy song song để set dependency cho gọn
    parallel_tasks = [
        task_basic_info,
        task_subcomp,
        task_lead,
        task_inc,
        task_bal,
        task_cf,
        task_price
    ]

    start >> [tickers_list, crawl_industry]

    tickers_list >> parallel_tasks

    # 3. Tất cả xong -> End
    [crawl_industry] + parallel_tasks >> end


dag_obj = stock_full_etl()
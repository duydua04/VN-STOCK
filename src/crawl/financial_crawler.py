import os

import pandas as pd
import time
from vnstock import Vnstock
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()

class VnstockDataCrawler:
    """
    Crawler du lieu tai chinh, thi truong.
    """
    def __init__(self, max_workers=5):
        self.max_workers = max_workers
        self.api_key = os.getenv("VNSTOCK_API_KEY")
        self.BATCH_SIZE = 60
        self.SLEEP_TIME = 65

    def _process_in_batches(self, stock_list, worker_func, label="Data"):
        all_results = []
        total = len(stock_list)

        for i in range(0, total, self.BATCH_SIZE):
            batch = stock_list[i: i + self.BATCH_SIZE]
            print(f"{label}: {i + 1}-{min(i + self.BATCH_SIZE, total)}/{total} mã")

            with ThreadPoolExecutor(self.max_workers) as ex:
                futures = {ex.submit(worker_func, s): s for s in batch}
                for f in as_completed(futures):
                    res = f.result()
                    if res is not None and not res.empty:
                        all_results.append(res)

            if i + self.BATCH_SIZE < total:
                print(f"Sleep {self.SLEEP_TIME}s")
                time.sleep(self.SLEEP_TIME)

        return pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()


    def fetch_price_history(self, stock_list: list, start_date: str, end_date: str):
        print(f"Tải lịch sử giá ({start_date} - {end_date})...")
        def _worker(symbol):
            try:
                df = Vnstock().stock(symbol=symbol, source='VCI').quote.history(start=start_date, end=end_date, interval='1D')
                if df is not None and not df.empty:
                    df['ticker'] = symbol
                    if 'time' in df.columns:
                        df = df.rename(columns={'time': 'trade_date'})
                    elif 'tradingDate' in df.columns:
                        df = df.rename(columns={'tradingDate': 'trade_date'})
                    return df
            except:
                return None

        return self._process_in_batches(stock_list, _worker, "Price")

    def fetch_financial_ratios(self, stock_list: list, period='quarter'):
        print(f"Tải chỉ số tài chính ({period})...")
        def _worker(symbol):
            try:
                df = Vnstock().stock(symbol=symbol, source='VCI') \
                    .finance.ratio(period=period, lang='vi')
                if df is not None and not df.empty:
                    df['ticker'] = symbol
                    return df

            except:
                return None
        return self._process_in_batches(stock_list, _worker, "Ratios")

    def fetch_financial_reports(self, stock_list: list, report_type='income_statement'):
        print(f"Tải BCTC ({report_type})...")
        def _worker(symbol):
            try:
                stock = Vnstock().stock(symbol=symbol, source='VCI')
                df = None
                if report_type == 'income_statement':
                    df = stock.finance.income_statement(period='quarter', lang='vi')
                elif report_type == 'balance_sheet':
                    df = stock.finance.balance_sheet(period='quarter', lang='vi')
                elif report_type == 'cash_flow':
                    df = stock.finance.cash_flow(period='quarter', lang='vi')

                if df is not None and not df.empty:
                    df['ticker'] = symbol
                    df['report_type'] = report_type
                    return df
            except:
                return None

        return self._process_in_batches(stock_list, _worker, "Finance Report")
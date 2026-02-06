import pandas as pd
from vnstock import Vnstock

class VnstockProfileCrawler:
    """
    Crawler lấy dữ liệu thô về Ban lãnh đạo và Cổ đông.
    """
    def __init__(self):
        try:
            self.vn = Vnstock()
        except Exception as e:
            print(f"Lỗi khởi tạo Vnstock Profile: {e}")
            self.vn = None

    def _get_stock_connection(self, symbol):
        if not self.vn:
            return None
        try:
            return self.vn.stock(symbol=symbol, source='VCI')
        except:
            return None

    def fetch_officers(self, symbol):
        """Lấy dữ liệu thô Ban lãnh đạo."""
        stock = self._get_stock_connection(symbol)
        if not stock:
            return None
        try:
            df = stock.company.officers()
            if df is not None and not df.empty:
                df['stock_code'] = symbol
                return df
            return None

        except:
            return None

    def fetch_shareholders(self, symbol):
        """Lấy dữ liệu thô Cổ đông."""
        stock = self._get_stock_connection(symbol)
        if not stock:
            return None
        try:
            df = stock.company.shareholders()
            if df is not None and not df.empty:
                df['stock_code'] = symbol
                return df
            return None
        except:
            return None
import random
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup


def get_industries_master_data():
    """Crawl danh mục Mã ngành nghề kinh doanh Việt Nam"""
    print("Đang tải Master Data Ngành nghề...")
    url = "https://esign.misa.vn/5873/ma-nganh-nghe-kinh-doanh/"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")
        tables = soup.find_all("table")

        if not tables: return pd.DataFrame()

        curr_group = ""
        lv_1, lv_4, names = [], [], []

        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cols = row.find_all("td")
                if not cols or len(cols) < 6: continue

                level_1_text = cols[0].text.strip()
                if level_1_text and level_1_text not in ["Cấp 1", "STT", ""]:
                    curr_group = level_1_text

                level_4_text = cols[3].text.strip()
                name_text = cols[5].text.strip()

                if level_4_text and level_4_text.isdigit():
                    lv_1.append(curr_group)
                    lv_4.append(level_4_text)
                    names.append(name_text)

        df = pd.DataFrame({"industry_group": lv_1, "industry_code": lv_4, "industry_name": names})
        print(f"{len(df)} ngành.")
        return df
    except Exception as e:
        print(f"Lỗi: {e}")
        return pd.DataFrame()

def get_enterprise_information(enterprise_code: str):
    time.sleep(random.uniform(2, 5))
    url = f"https://finance.vietstock.vn/{enterprise_code}/ho-so-doanh-nghiep.htm"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://finance.vietstock.vn/"
    }
    try:
        response = requests.get(url, headers=headers, timeout=60)
        soup = BeautifulSoup(response.content, "html5lib")

        company_full_name = None
        h1_tag = soup.find("h1", class_="h1-title")
        if h1_tag:
            name_b_tag = h1_tag.find("b")
            if name_b_tag:
                company_full_name = name_b_tag.get_text(strip=True)
            else:
                company_full_name = h1_tag.get_text(strip=True).split(" (")[0]

        company_eng_name = None
        h2_tag = soup.find("h2", class_="title-2")
        if h2_tag:
            company_eng_name = h2_tag.get_text(strip=True)

        listing_date = None
        niem_yet_div = soup.find("div", id="niem-yet")
        if niem_yet_div:
            for tr in niem_yet_div.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 2 and "Ngày giao dịch đầu tiên" in tds[0].get_text():
                    listing_date = tds[1].get_text(strip=True)
                    break

        ans = {}
        info_section = soup.find("div", id="thanh-lap-cong-ty")
        if info_section:
            for tr in info_section.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) == 2:
                    key = tds[0].get_text(strip=True).replace("• ", "")
                    val = tds[1].get_text(strip=True)
                    ans[key] = val

        return (
            company_full_name,
            company_eng_name,
            ans.get('Mã số thuế'),
            ans.get('Địa chỉ'),
            ans.get('Email'),
            ans.get('Website'),
            listing_date
        )
    except Exception as e:
        print(f"Lỗi bóc tách {enterprise_code}: {e}")
        return None, None, None, None, None, None, None

def get_all_enterprise_information(df: pd.DataFrame):
    print("Crawling thông tin từ Vietstock chuẩn")
    if 'code' not in df.columns and 'stock_code' in df.columns:
        df['code'] = df['stock_code']

    df[['company_name', 'company_eng_name', 'tax_code', 'address', 'email', 'website', 'listing_date']] \
        = df['code'].apply(get_enterprise_information).apply(pd.Series)
    return df

def get_sub_companies(df: pd.DataFrame):
    print("Crawling công ty con")
    all_sub_comps = []
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://cafef.vn/"
    }

    for index, row in df.iterrows():
        stock_code = row['code'] if 'code' in row else row['stock_code']

        try:
            api_url = f"https://cafef.vn/du-lieu/Ajax/PageNew/GetDataSubsidiaries.ashx?Symbol={stock_code}"
            resp = requests.get(api_url, headers=headers, timeout=10)

            if resp.status_code == 200:
                data_json = resp.json()
                if data_json.get("Success"):
                    data = data_json.get("Data", {})

                    def append_data(source_list, type_code):
                        if not source_list: return
                        for item in source_list:
                            all_sub_comps.append({
                                "stock_code": stock_code,
                                "comp_name": item.get("Name"),
                                "comp_type": type_code,
                                "charter_capital": item.get("TotalCapital"),
                                "ownership_ratio": item.get("OwnershipRate")
                            })

                    append_data(data.get("Subsidiaries"), "sub")
                    append_data(data.get("AssociatedCompanies"), "aff")
        except Exception:
            pass

    if all_sub_comps:
        return pd.DataFrame(all_sub_comps)
    return pd.DataFrame()
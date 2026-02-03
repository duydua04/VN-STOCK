import re
import time
import unicodedata
import pandas as pd
import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from DrissionPage import ChromiumPage, ChromiumOptions


def process_url(url_str: str, comp_list: list = None):
    company_codes = []
    company_names = []
    company_exchanges = []
    cafef_url = []

    option = webdriver.ChromeOptions()
    option.add_argument("--headless")
    option.add_argument("--window-size=1920,1080")

    # Fake User-Agent để tránh bị chặn
    option.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(options=option)
    done = 0

    try:
        print(f"Selenium đang truy cập: {url_str}")
        driver.get(url_str)

        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "table-data-business"))
        )

        for count in range(84):
            print(f"Page {count + 1}...")
            html = driver.page_source

            try:
                c_codes, c_names, c_excs, c_url = extract_companies(html, comp_list)
                if c_codes:
                    print(f"Tìm thấy: {c_codes}")
                    company_codes.extend(c_codes)
                    company_names.extend(c_names)
                    company_exchanges.extend(c_excs)
                    cafef_url.extend(c_url)
                    done += len(c_codes)
            except ValueError:
                pass

            if comp_list is not None and done >= len(comp_list):
                print("Đã lấy đủ danh sách yêu cầu.")
                break

            try:
                next_button = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.ID, "paging-right"))
                )
                driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", next_button)
                time.sleep(2)
            except TimeoutException:
                print("Đã hết trang hoặc không tìm thấy nút Next.")
                break
            except Exception as e:
                print(f"Lỗi chuyển trang: {e}")
                break

    except Exception as e:
        print(f"Lỗi Selenium: {e}")
    finally:
        driver.quit()

    return pd.DataFrame({
        "code": company_codes,
        "name": company_names,
        "exchange": company_exchanges,
        "cafef_url": cafef_url
    })


def get_sub_companies(df: pd.DataFrame):
    all_sub_comps = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://cafef.vn/"
    }

    for index, row in df.iterrows():
        stock_code = row['code']

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
                                "contributed_capital": item.get("SharedCapital"),
                                "ownership_ratio": item.get("OwnershipRate")
                            })

                    append_data(data.get("Subsidiaries"), "sub")
                    append_data(data.get("AssociatedCompanies"), "aff")
                    print(f"Tổng: {len(all_sub_comps)} bản ghi.")
                else:
                    print("Không có dữ liệu công ty con.")
            else:
                print(f"Lỗi HTTP {resp.status_code}")
        except Exception as e:
            print(f"Lỗi API: {e}")

    if all_sub_comps:
        return pd.DataFrame(all_sub_comps)
    return pd.DataFrame(
        columns=["stock_code", "comp_name", "comp_type", "charter_capital", "contributed_capital", "ownership_ratio"])


def get_enterprise_industry_based_on_tax_code(tax_code: str | None, company_name: str):
    if not tax_code or str(tax_code) == "nan": return None
    print(f"🕵️ Tra cứu TVPL (DrissionPage): {tax_code}")

    # Cấu hình DrissionPage
    co = ChromiumOptions()
    co.auto_port()
    co.set_argument('--no-first-run')
    page = ChromiumPage(addr_or_opts=co)

    try:
        search_url = f"https://thuvienphapluat.vn/ma-so-thue/tra-cuu-ma-so-thue-doanh-nghiep?timtheo=ma-so-thue&tukhoa={tax_code}"
        page.get(search_url)

        if page.ele("@text():Verify you are human", timeout=3):
            time.sleep(5)

        if page.ele('tag:a@@href:-mst-', timeout=5):
            page.ele('tag:a@@href:-mst-').click()
            page.wait.load_start()

            possible_codes = page.eles('.col-md-2')
            for div in possible_codes:
                text = div.text.strip()
                if re.match(r'^\d{4}$', text):
                    print(f"Success: {text}")
                    return text

            match = re.search(r'Mã ngành.*?>(\d{4})<', page.html, re.DOTALL)
            if match: return match.group(1)
        else:
            print("Không tìm thấy link chi tiết.")

    except Exception as e:
        print(f"Lỗi DrissionPage: {e}")
    finally:
        page.quit()
    return "Not Found"


def extract_companies(html_str: str, comp_list: list = None):
    soup = BeautifulSoup(html_str, "html.parser")
    table = soup.find("table", class_="table-data-business")
    if not table: raise ValueError("No table found")

    company_codes = []
    company_names = []
    company_exchanges = []
    cafef_urls = []

    rows = table.find_all("tr")
    for row in rows:
        cols = row.find_all("td")
        if not cols: continue

        code = cols[0].get_text(strip=True)
        name = cols[1].get_text(strip=True)
        exchange = cols[3].get_text(strip=True)

        if comp_list and code not in comp_list: continue

        company_codes.append(code)
        company_names.append(name)
        company_exchanges.append(exchange)
        link_tag = cols[0].find("a")
        url = link_tag.get("href") if link_tag else None
        cafef_urls.append(url)

    return company_codes, company_names, company_exchanges, cafef_urls


def get_vn30_list():
    url = "https://topi.vn/vn30-la-gi.html"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            table = soup.find("table")
            if table:
                answer = []
                for row in table.find_all("tr"):
                    cols = row.find_all("td")
                    if len(cols) > 1:
                        answer.append(cols[1].get_text(strip=True))
                return pd.DataFrame({"stock_code": answer[1:]})
    except Exception as e:
        print(f"Lỗi lấy VN30: {e}")

    return pd.DataFrame({"stock_code": ["ACB", "BCM", "BID", "BVH", "CTG", "FPT", "GAS", "GVR", "HDB", "HPG", "MBB",
                                        "MSN", "MWG", "PLX", "POW", "SAB", "SHB", "SSB", "SSI", "STB", "TCB", "TPB",
                                        "VCB", "VHM", "VIB", "VIC", "VJC", "VNM", "VPB", "VRE"]})


def get_enterprise_information(enterprise_code: str):
    url = f"https://finance.vietstock.vn/{enterprise_code}/ho-so-doanh-nghiep.htm"
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.vietstock.vn/"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.content, "html5lib")
        listing_date = None

        niem_yet = soup.find("div", id="niem-yet")
        if niem_yet:
            rows = niem_yet.find("table").find("tbody").find_all("tr")
            if rows: listing_date = rows[0].find_all("td")[1].text.strip()

        ans = {}
        info_div = soup.find("div", id="thanh-lap-cong-ty")
        if info_div:
            rows = info_div.find("table").find("tbody").find_all("tr")
            for row in rows:
                cols = row.find_all("td")
                if len(cols) == 2:
                    ans[cols[0].text.strip().replace("• ", "")] = cols[1].text.strip()

        return ans.get('Mã số thuế'), ans.get('Địa chỉ'), ans.get('Điện thoại'), ans.get('Email'), ans.get(
            'Website'), listing_date
    except:
        return None, None, None, None, None, None


def get_all_enterprise_information(df: pd.DataFrame):
    df[['tax_code', 'address', 'phone_number', 'email', 'website', 'listing_date']] \
        = df['code'].apply(get_enterprise_information).apply(pd.Series)
    return df


def get_all_enterprises_industry_code(df: pd.DataFrame):
    df['industry_code'] = df.apply(lambda row: get_enterprise_industry_based_on_tax_code(row['tax_code'], row['name']),
                                   axis=1)
    return df


def remove_vn_diacritics(text: str) -> str:
    normalized = unicodedata.normalize("NFD", text)
    no_diacritics = "".join(c for c in normalized if unicodedata.category(c) != "Mn")
    replacements = {"Đ": "D", "đ": "d"}
    return re.sub("|".join(replacements.keys()), lambda m: replacements[m.group()], no_diacritics)
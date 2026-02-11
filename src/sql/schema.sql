
-- 1.1. Bảng Ngành nghề (Nguồn: Misa Master Data)
CREATE TABLE IF NOT EXISTS dim_industry (
    industry_code VARCHAR(20) PRIMARY KEY, -- Mã cấp 4 (VD: 0111, 6190)
    industry_group VARCHAR(10),            -- Mã cấp 1 (VD: A, C, J)
    industry_name VARCHAR(255),            -- Tên ngành (VD: Hoạt động viễn thông)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 1.2. Bảng Doanh nghiệp (Nguồn: Vietstock + Vnstock)
-- Đây là bảng trung tâm của Dimension
CREATE TABLE IF NOT EXISTS dim_enterprise (
    stock_code VARCHAR(10) PRIMARY KEY,    -- Mã CK (VD: FPT, HPG) - Primary Key
    company_name VARCHAR(255),             -- Tên đầy đủ
    company_eng_name VARCHAR(255),
    tax_code VARCHAR(20),                  -- Mã số thuế
    listing_date DATE,                     -- Ngày niêm yết
    address TEXT,
    website VARCHAR(255),
    email VARCHAR(100),
    industry_code VARCHAR(20),             -- Foreign Key tới dim_industry

    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_ent_industry FOREIGN KEY (industry_code) REFERENCES dim_industry(industry_code)
);

-- 1.3. Bảng Đối tượng Sở hữu (Nguồn: Vnstock VCI - Merge Lãnh đạo & Cổ đông)
-- Spark sẽ xử lý logic để điền is_organization
CREATE TABLE IF NOT EXISTS dim_holder_info (
    holder_id VARCHAR(50) PRIMARY KEY,     -- ID gốc từ VCI (VD: 16, 97903843)
    full_name VARCHAR(255),                -- Tên (Trương Gia Bình, SCIC...)
    is_organization BOOLEAN DEFAULT FALSE, -- Cờ: TRUE (Tổ chức), FALSE (Cá nhân)
    updated_at DATE DEFAULT CURRENT_DATE
);

-- 1.4. Bảng Thời gian (Date Dimension)
-- Bảng này thường được generate bằng script, giúp query theo Quý/Năm dễ hơn
CREATE TABLE IF NOT EXISTS dim_date (
    date_id DATE PRIMARY KEY,
    day INT,
    month INT,
    quarter INT,
    year INT,
    is_weekend BOOLEAN,
    day_of_week VARCHAR(10)
);

-- =============================================================================
-- 2. FACT TABLES (Bảng Dữ liệu - Chứa số liệu biến động theo thời gian)
-- =============================================================================

-- 2.1. Fact Lịch sử Giá & Giao dịch (Daily)
-- Nguồn: Vnstock Price History
CREATE TABLE IF NOT EXISTS fact_price_history (
    price_id SERIAL PRIMARY KEY,
    stock_code VARCHAR(10) NOT NULL,
    trade_date DATE NOT NULL,

    open DECIMAL(15, 2),
    high DECIMAL(15, 2),
    low DECIMAL(15, 2),
    close DECIMAL(15, 2),
    volume BIGINT,
    market_cap DECIMAL(20, 2), -- Vốn hóa (nếu crawl được hoặc Spark tính close * shares)

    CONSTRAINT fk_price_stock FOREIGN KEY (stock_code) REFERENCES dim_enterprise(stock_code),
    CONSTRAINT uq_price_daily UNIQUE (stock_code, trade_date) -- Một mã 1 ngày chỉ có 1 dòng
);

-- 2.2. Fact Chỉ số Tài chính (Quarterly/Yearly)
-- Nguồn: Vnstock Ratios (PE, PB, ROE...)
CREATE TABLE IF NOT EXISTS fact_financial_ratio (
    ratio_id SERIAL PRIMARY KEY,
    stock_code VARCHAR(10) NOT NULL,
    report_period VARCHAR(20), -- VD: Q4/2024
    year INT,
    quarter INT,

    pe DECIMAL(10, 2),         -- Price to Earning
    pb DECIMAL(10, 2),         -- Price to Book
    roe DECIMAL(10, 4),        -- Return on Equity
    roa DECIMAL(10, 4),        -- Return on Assets
    eps DECIMAL(15, 2),        -- Earning Per Share
    net_margin DECIMAL(10, 4), -- Biên lợi nhuận ròng
    book_value DECIMAL(15, 2), -- Giá trị sổ sách

    CONSTRAINT fk_ratio_stock FOREIGN KEY (stock_code) REFERENCES dim_enterprise(stock_code),
    CONSTRAINT uq_ratio_period UNIQUE (stock_code, year, quarter)
);

-- 2.3. Fact Báo cáo Tài chính (Core Metrics)
-- Nguồn: Vnstock Financial Report (Income, Balance Sheet)
CREATE TABLE IF NOT EXISTS fact_financial_report (
    report_id SERIAL PRIMARY KEY,
    stock_code VARCHAR(10) NOT NULL,
    report_type VARCHAR(20),   -- 'IncomeStatement', 'BalanceSheet'
    year INT,
    quarter INT,

    -- Các chỉ tiêu quan trọng (Cần Spark map từ raw data vào đây)
    revenue DECIMAL(20, 2),       -- Doanh thu thuần
    gross_profit DECIMAL(20, 2),  -- Lợi nhuận gộp
    operating_profit DECIMAL(20, 2), -- LN từ HĐKD
    net_profit DECIMAL(20, 2),    -- Lợi nhuận sau thuế (LN ròng)

    total_assets DECIMAL(20, 2),      -- Tổng tài sản
    total_equity DECIMAL(20, 2),      -- Vốn chủ sở hữu
    total_liabilities DECIMAL(20, 2), -- Tổng nợ phải trả
    short_term_debt DECIMAL(20, 2),   -- Vay ngắn hạn
    long_term_debt DECIMAL(20, 2),    -- Vay dài hạn

    CONSTRAINT fk_fin_stock FOREIGN KEY (stock_code) REFERENCES dim_enterprise(stock_code),
    CONSTRAINT uq_fin_report UNIQUE (stock_code, report_type, year, quarter)
);

-- 2.4. Fact Sở hữu (Ownership Snapshot)
-- Nguồn: Vnstock Profile (Merge data từ Officers & Shareholders)
CREATE TABLE IF NOT EXISTS fact_ownership (
    ownership_id SERIAL PRIMARY KEY,
    stock_code VARCHAR(10) NOT NULL,
    holder_id VARCHAR(50) NOT NULL,

    position VARCHAR(255),           -- Chức vụ (Chủ tịch, Cổ đông lớn...)
    shares_count BIGINT,             -- Số lượng nắm giữ
    ownership_ratio DECIMAL(10, 6),  -- Tỷ lệ (0.05 = 5%)

    report_date DATE,                -- Ngày dữ liệu được cập nhật từ nguồn

    CONSTRAINT fk_own_stock FOREIGN KEY (stock_code) REFERENCES dim_enterprise(stock_code),
    CONSTRAINT fk_own_holder FOREIGN KEY (holder_id) REFERENCES dim_holder_info(holder_id)
    -- Lưu ý: Không để Unique ở đây vì một người có thể thay đổi số lượng theo thời gian
);

-- 2.5. Fact Công ty con & Liên kết
-- Nguồn: CafeF (Hàm get_sub_companies)
CREATE TABLE IF NOT EXISTS fact_subsidiary (
    sub_id SERIAL PRIMARY KEY,
    parent_stock_code VARCHAR(10) NOT NULL,   -- Công ty mẹ (VD: FPT)

    subsidiary_name VARCHAR(255),    -- Tên công ty con
    subsidiary_type VARCHAR(50),     -- 'Công ty con' hoặc 'Công ty liên kết'
    ownership_ratio DECIMAL(10, 2),  -- Tỷ lệ sở hữu của mẹ (%)
    charter_capital DECIMAL(20, 2),  -- Vốn điều lệ

    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_sub_parent FOREIGN KEY (parent_stock_code) REFERENCES dim_enterprise(stock_code)
);

-- Index cho tìm kiếm theo mã CK
CREATE INDEX idx_price_stock ON fact_price_history(stock_code);
CREATE INDEX idx_ratio_stock ON fact_financial_ratio(stock_code);
CREATE INDEX idx_fin_stock ON fact_financial_report(stock_code);

-- Index cho tìm kiếm theo thời gian (Time-series analysis)
CREATE INDEX idx_price_date ON fact_price_history(trade_date);
CREATE INDEX idx_ratio_year ON fact_financial_ratio(year);
CREATE INDEX idx_fin_year ON fact_financial_report(year);

-- Index cho join sở hữu
CREATE INDEX idx_own_holder ON fact_ownership(holder_id);
import pandas as pd


def get_vn50_list():
    """
    Trả về danh sách 50 mã cổ phiếu lớn nhất (VN30 + 20 mã tiềm năng).
    Danh sách này cố định (Hardcoded) để đảm bảo ETL ổn định.
    """
    print("📋 [Task 0] Đang lấy danh sách VN50...")

    # 1. Nhóm VN30
    vn30 = [
        "ACB", "BCM", "BID", "BVH", "CTG", "FPT", "GAS", "GVR", "HDB", "HPG",
        "MBB", "MSN", "MWG", "PLX", "POW", "SAB", "SHB", "SSB", "SSI", "STB",
        "TCB", "TPB", "VCB", "VHM", "VIB", "VIC", "VJC", "VNM", "VPB", "VRE"
    ]

    next_20 = [
        "DGC", "LPB", "EIB", "PNJ", "REE",
        "VIX", "VND", "HCM", "VCI",
        "KBC", "KDH", "NLG", "PDR", "DIG",
        "DXG", "GEX", "GMD",
        "VHC", "FRT", "DPM"
    ]

    full_list = sorted(list(set(vn30 + next_20)))

    print(f"Đã load xong {len(full_list)} mã VN50.")
    return pd.DataFrame({"stock_code": full_list})
import os
import time
import pickle
import logging
import pandas as pd
from datetime import datetime, timedelta
import gspread
import numpy as np

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

# Import the updated Chart_Test function
from Chart_Test import run_bbp_chart_analysis

# Import WebscraperS2
from WebscraperS2 import scrape_main_page

# Undetected ChromeDriver
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ----------------------------------------------
# LOGGING SETUP
# ----------------------------------------------
LOG_FILE = "asin_fetch_log.txt"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("RawDataCollector")

# ----------------------------------------------
# GOOGLE SHEETS CONFIG
# ----------------------------------------------
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
CLIENT_SECRET_FILE = r"C:\Users\Luke\Desktop\Automation\client_secret_451363787295-6aivcb277tha61cut64a8l288ui8hnba.apps.googleusercontent.com.json"

SPREADSHEET_ID = "1zEEx13R7WH6AGxd7jetlckwRB5bpBAAvMxGa4r8uV3M"  # "Scraped Data"
TARGET_TAB_NAME = "Scraped Data"

# Product Database + Tabs
SPREADSHEET_NAME = "Amazon Supplier Process"
SHEET_TAB_NAME = "Product Database"
VAT_SHEET_NAME = "Amazon Orders API 2024"
VAT_TAB_NAME = "VAT Exemptions"
STOCK_SHEET_NAME = "Daily Stock Data"
STOCK_TAB_NAME = "Stock Data"
SALES_SHEET_NAME = "Amazon Orders API 2024"
SALES_TAB_NAME = "New Orders"


# ----------------------------------------------
# 1) UTILITY: ensure_gs_creds
# ----------------------------------------------
def ensure_gs_creds():
    creds = None
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as tok:
            creds = pickle.load(tok)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.pickle", "wb") as tok:
            pickle.dump(creds, tok)
    return creds


# ----------------------------------------------
# 2) get_active_asins
# ----------------------------------------------
def get_active_asins():
    creds = ensure_gs_creds()
    client = gspread.authorize(creds)
    logger.info("🔗 Connected to Google Sheets for Product Database.")

    # VAT Exempt
    vat_sheet = client.open(VAT_SHEET_NAME).worksheet(VAT_TAB_NAME)
    vat_exempt_skus = vat_sheet.col_values(1)[1:]
    vat_exempt_set = {sku.strip() for sku in vat_exempt_skus if sku.strip()}

    # Product Database
    sheet = client.open(SPREADSHEET_NAME).worksheet(SHEET_TAB_NAME)
    rows = sheet.get_all_records(head=2)

    results = []
    for r in rows:
        asin_raw = str(r.get("Asin", "")).strip().upper()
        sku = str(r.get("SKU", "")).strip()
        cost_str = str(r.get("CPU", "")).replace("£", "").strip()
        is_dropped_str = str(r.get("D/C", "")).strip().lower()

        try:
            cost_val = float(cost_str)
        except:
            cost_val = None

        is_dropped = (is_dropped_str in ["true", "yes", "✓", "1"])
        vat_rate = 0 if sku in vat_exempt_set else 20

        if asin_raw and cost_val and not is_dropped:
            results.append({
                "asin": asin_raw,
                "sku": sku,
                "cost": cost_val,
                "vat": vat_rate
            })
    logger.info(f"✅ Found {len(results)} active ASINs in Product Database.")
    return results


# ----------------------------------------------
# 3) get_daily_stock_data
# ----------------------------------------------
def get_daily_stock_data():
    creds = ensure_gs_creds()
    client = gspread.authorize(creds)
    logger.info("🔗 Connected to Google Sheets for daily stock data.")

    sheet = client.open(STOCK_SHEET_NAME).worksheet(STOCK_TAB_NAME)
    rows = sheet.get_all_records()

    df = pd.DataFrame(rows)
    if "Timestamp" in df.columns:
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
        df["Date"] = df["Timestamp"].dt.strftime("%Y-%m-%d")
    else:
        df["Date"] = ""

    if "FBA/FBM Stock" in df.columns:
        df.rename(columns={"FBA/FBM Stock": "Stock"}, inplace=True)
    else:
        df["Stock"] = 0

    needed = ["ASIN", "Date", "Stock"]
    for n in needed:
        if n not in df.columns:
            df[n] = 0

    df = df[needed].copy()
    df.sort_values(by=["ASIN", "Date"], inplace=True)
    df["Stock"] = pd.to_numeric(df["Stock"], errors="coerce").fillna(0).astype(int)
    logger.info(f"✅ Loaded {len(df)} stock rows.")
    return df


# ----------------------------------------------
# 4) get_real_sales_data
# ----------------------------------------------
def get_real_sales_data():
    creds = ensure_gs_creds()
    client = gspread.authorize(creds)
    logger.info("🔗 Connected to Google Sheets for real sales data.")

    sheet = client.open(SALES_SHEET_NAME).worksheet(SALES_TAB_NAME)
    rows = sheet.get_all_records()

    df = pd.DataFrame(rows)
    if "Date" not in df.columns:
        df["Date"] = ""
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")

    if "Order Status" in df.columns:
        df = df[df["Order Status"].str.lower() == "shipped"].copy()

    q_col = "Quantity Ordered"
    p_col = "Price"
    d_col = "Delivery/GiftWrap"

    for c in [q_col, p_col, d_col]:
        if c not in df.columns:
            df[c] = 0

    df[q_col] = pd.to_numeric(df[q_col], errors="coerce").fillna(0)
    df[p_col] = pd.to_numeric(df[p_col], errors="coerce").fillna(0)

    def parse_delivery(x):
        if pd.isna(x):
            return 0
        s = str(x).strip()
        if s.startswith("(") and s.endswith(")"):
            return 0
        try:
            return float(s.replace("(", "").replace(")", ""))
        except:
            return 0

    df[d_col] = df[d_col].apply(parse_delivery)

    # Map SKU->ASIN
    active_asins = get_active_asins()
    sku_map = {itm["sku"]: itm["asin"] for itm in active_asins}
    if "SKU" not in df.columns:
        df["SKU"] = ""
    df["ASIN"] = df["SKU"].map(sku_map).fillna("")

    df = df[df["ASIN"] != ""]

    df["Q"] = df[q_col].replace(0, float("nan"))
    df["NetPrice"] = (df[p_col] - df[d_col]) / df["Q"]

    grp = df.groupby(["ASIN", "Date"], as_index=False).agg({
        q_col: "sum",
        "NetPrice": "mean"
    })
    grp.rename(columns={
        q_col: "Actual Sales Volume",
        "NetPrice": "Avg Real Price"
    }, inplace=True)
    logger.info(f"✅ Merged real sales for {len(grp)} date combos.")
    return grp[["ASIN", "Date", "Actual Sales Volume", "Avg Real Price"]]


# ----------------------------------------------
# 5) generate_365_day_range
# ----------------------------------------------
def generate_365_day_range():
    today = datetime.now().date()
    start_date = today - timedelta(days=364)
    date_list = []
    cur = start_date
    while cur <= today:
        date_list.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return date_list


# ----------------------------------------------
# MAIN
# ----------------------------------------------
if __name__ == "__main__":

    # 1) Setup Google Sheets
    creds = ensure_gs_creds()
    client = gspread.authorize(creds)
    ws = client.open_by_key(SPREADSHEET_ID).worksheet(TARGET_TAB_NAME)

    # Clear & set final headers
    ws.clear()
    HEADERS = [
        "Date", "ASIN", "CPU", "VAT %",
        "Category", "Stock", "Actual Sales Volume", "Avg Real Price",
        "Buy Box Price", "FBA Price", "FBM Price", "Amazon Price",
        "Chosen Price", "Chosen Source", "Break-even Price", "BSR",
        "Review Count", "Offer Count",
        "Scraper2_Title", "Scraper2_MonthlySold", "Scraper2_Rating",
        "Scraper2_ParentTotalReviews", "Scraper2_VariantReviews",
        "Review Share %"
    ]
    ws.append_row(HEADERS, value_input_option="USER_ENTERED")
    logger.info(f"✅ Cleared '{TARGET_TAB_NAME}' and inserted final headers.")

    # 2) Gather local data
    asins = get_active_asins()
    df_stock = get_daily_stock_data()
    df_sales = get_real_sales_data()
    date_list = generate_365_day_range()

    # 3) Start driver
    from undetected_chromedriver import Chrome, ChromeOptions
    options = ChromeOptions()
    options.add_argument(r"--user-data-dir=C:\Users\Luke\AppData\Local\Google\Chrome\User Data")
    options.add_argument(r"--profile-directory=Profile 5")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--start-maximized")

    driver = Chrome(
        options=options,
        user_data_dir=options._user_data_dir,
        version_main=None,
        driver_executable_path=r"C:\Users\Luke\appdata\roaming\undetected_chromedriver\undetected_chromedriver.exe",
        browser_executable_path=r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
    )
    logger.info("✅ Shared Chrome driver started.")

    # 4) Process each ASIN
    for itm in asins:
        asin = itm["asin"]
        cost = itm["cost"]
        vat_val = itm["vat"]
        logger.info(f"Processing ASIN={asin} CPU={cost} VAT={vat_val}...")

        try:
            # A) Pull ~365 chart data from BBP
            df_bbp = run_bbp_chart_analysis(asin, cost, vat_val, driver=driver, days=365)

            # Adjust columns
            col_map = {
                "BreakEven": "Break-even Price",
                "break_even": "Break-even Price",
                "chosen_source": "Chosen Source",
                "Source": "Chosen Source",
                "bsr": "BSR",
                "review_count": "Review Count",
                "offer_count": "Offer Count",
            }
            rename_dict = {}
            for k, v in col_map.items():
                if k in df_bbp.columns:
                    rename_dict[k] = v
            df_bbp.rename(columns=rename_dict, inplace=True)

            # Force exist
            force_cols = [
                "Break-even Price", "BSR", "Review Count", "Offer Count",
                "Chosen Source", "Buy Box Price", "Category"
            ]
            for fc in force_cols:
                if fc not in df_bbp.columns:
                    df_bbp[fc] = ""

            # B) We'll create a 365-date base and left-merge the BBP daily
            df_dates = pd.DataFrame({"Date": date_list})
            # left-merge => so we keep full 365 in df_365
            df_365 = pd.merge(
                df_dates, df_bbp,
                on="Date", how="left", suffixes=("", "_bbp")
            )

            # For Break-even Price => fill with the single value if present
            # or "N/A" if missing
            if "Break-even Price" in df_365.columns:
                # If there's at least one row with a break-even
                if df_365["Break-even Price"].notna().any():
                    be_series = df_365["Break-even Price"].dropna()
                    if not be_series.empty:
                        # pick the first non-null as the official
                        official_be = be_series.iloc[0]
                        # If it's an empty string, set "N/A"
                        if str(official_be).strip() == "":
                            official_be = "N/A"
                        df_365["Break-even Price"] = official_be
                    else:
                        df_365["Break-even Price"] = "N/A"
                else:
                    df_365["Break-even Price"] = "N/A"
            else:
                df_365["Break-even Price"] = "N/A"

            # Category => fill once if found, else "Unknown"
            if "Category" in df_365.columns:
                non_null_cat = df_365["Category"].dropna()
                if not non_null_cat.empty:
                    cat_val = non_null_cat.iloc[0] if str(non_null_cat.iloc[0]).strip() else "Unknown"
                    df_365["Category"] = cat_val
                else:
                    df_365["Category"] = "Unknown"
            else:
                df_365["Category"] = "Unknown"

            # C) Insert ASIN/CPU/VAT
            df_365["ASIN"] = asin
            df_365["CPU"] = cost
            df_365["VAT %"] = vat_val

            # D) Merge stock
            df_365 = pd.merge(
                df_365, df_stock,
                on=["ASIN", "Date"], how="left", suffixes=("", "_stock")
            )
            df_365["Stock"] = df_365["Stock"].fillna(0).astype(int)

            # E) Merge real sales
            df_365 = pd.merge(
                df_365, df_sales,
                on=["ASIN", "Date"], how="left", suffixes=("", "_sales")
            )
            df_365["Actual Sales Volume"] = df_365["Actual Sales Volume"].fillna(0).astype(int)
            df_365["Avg Real Price"] = df_365["Avg Real Price"].fillna(0)

            # F) WebscraperS2 once
            product_url = f"https://www.amazon.co.uk/dp/{asin}"
            driver.get(product_url)
            s2_data = scrape_main_page(driver)

            # Overwrite in each row
            df_365["Scraper2_Title"] = s2_data.get("main_title", "N/A")
            df_365["Scraper2_MonthlySold"] = s2_data.get("monthly_sold", "0")
            df_365["Scraper2_Rating"] = s2_data.get("rating", "0")
            df_365["Scraper2_ParentTotalReviews"] = s2_data.get("parent_total_reviews", "0")
            # If variant not in columns, create
            if "Scraper2_VariantReviews" not in df_365.columns:
                df_365["Scraper2_VariantReviews"] = s2_data.get("variant_reviews", "0")
            else:
                # fill from S2 if blank
                df_365["Scraper2_VariantReviews"] = s2_data.get("variant_reviews", "0")

            # G) Compute "Review Share %" => variant / parent
            try:
                var_r = pd.to_numeric(df_365["Scraper2_VariantReviews"], errors="coerce").fillna(0)
                par_r = pd.to_numeric(df_365["Scraper2_ParentTotalReviews"], errors="coerce").fillna(0)
                share = (var_r / par_r * 100).replace([float("inf"), -float("inf")], 0)
                df_365["Review Share %"] = share.fillna(0)
            except:
                df_365["Review Share %"] = 0

            # H) Final reorder & fill missing numeric with 0
            final_cols = [
                "Date", "ASIN", "CPU", "VAT %",
                "Category", "Stock", "Actual Sales Volume", "Avg Real Price",
                "Buy Box Price", "FBA Price", "FBM Price", "Amazon Price",
                "Chosen Price", "Chosen Source", "Break-even Price", "BSR",
                "Review Count", "Offer Count",
                "Scraper2_Title", "Scraper2_MonthlySold", "Scraper2_Rating",
                "Scraper2_ParentTotalReviews", "Scraper2_VariantReviews",
                "Review Share %"
            ]
            for c in final_cols:
                if c not in df_365.columns:
                    df_365[c] = 0 if c in ["Stock", "Actual Sales Volume"] else ""

            # Convert any NaN => 0 or "N/A" for numeric vs string
            for c in df_365.columns:
                if df_365[c].dtype.kind in ["f", "i"]:  # numeric
                    df_365[c] = df_365[c].fillna(0)
                else:
                    df_365[c] = df_365[c].fillna("")

            # I) Create the list-of-lists
            out_rows = []
            for _, rw in df_365.iterrows():
                row_data = []
                for col_name in final_cols:
                    val = rw.get(col_name, "")
                    if pd.isna(val):
                        val = 0 if col_name in ["Stock", "Actual Sales Volume"] else ""
                    # Replace NaN or infinite values with 0
                    if isinstance(val, float) and not np.isfinite(val):
                        val = 0
                    row_data.append(str(val) if col_name not in ["Stock", "Actual Sales Volume"] else val)
                out_rows.append(row_data)


            # 7) Append to Google Sheets in small batches to avoid JSON errors
            BATCH_SIZE = 50
            for start_i in range(0, len(out_rows), BATCH_SIZE):
                sub_rows = out_rows[start_i:start_i+BATCH_SIZE]
                ws.append_rows(sub_rows, value_input_option="USER_ENTERED")
            logger.info(f"✅ Appended {len(out_rows)} daily rows for ASIN={asin}.")

        except Exception as exc:
            logger.error(f"❌ Error for ASIN={asin}: {exc}")

    # Close
    driver.quit()
    logger.info("✅ All done!")

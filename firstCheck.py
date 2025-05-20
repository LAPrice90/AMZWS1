# firstCheck.py

# ---------------------------------------------------
# 1. IMPORTS
# ---------------------------------------------------
# Standard libraries
import os
import time
import pickle
import logging

# Third-party libraries
import requests
import gspread
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

# Internal modules
from tokenCall import get_access_token
from amazonCatalogCall import get_catalog_details
from hazmatCall import check_eligibility_for_asin
from pricingCall import get_pricing_details_for_asin
from feeCall import get_fees_estimate_for_asin
from Webscrape import process_passed_product

# ---------------------------------------------------
# 2. CONFIGURATION
# ---------------------------------------------------
LOG_FILE = "script1_log.txt"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Script1")
logger.info("Logging setup complete.")

MAX_RETRIES = 6
RETRY_DELAY = 30
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
CLIENT_SECRET_FILE = "client_secret.json"

# ---------------------------------------------------
# 3. GOOGLE SHEETS RETRY HELPER
# ---------------------------------------------------
def retry_google_sheets_operation(func, *args, **kwargs):
    """
    Retries any Google Sheets API operation multiple times if there's a quota
    exceeded or a transient error.
    """
    for attempt in range(MAX_RETRIES):
        try:
            return func(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            if "Quota exceeded" in str(e):
                logger.info(
                    f"Quota exceeded. Retrying in {RETRY_DELAY} seconds... "
                    f"(Attempt {attempt + 1}/{MAX_RETRIES})"
                )
                time.sleep(RETRY_DELAY)
            else:
                raise
        except Exception as e:
            logger.info(f"Unexpected error during Google Sheets operation: {e}")
            raise
    logger.info("Max retries reached. Operation failed.")
    return None

# ---------------------------------------------------
# 4. HELPER FUNCTIONS
# ---------------------------------------------------
def check_a1_status(sheet):
    """
    Checks the value of cell A1 to decide if we continue or stop:
      - If it's "completed", we return the string "completed".
      - If it's a digit, we return it as an integer.
      - Otherwise, returns None.
    """
    try:
        value = retry_google_sheets_operation(sheet.cell, 1, 1).value
        if not value:
            return None

        value_str = value.strip().lower()
        if value_str == "completed":
            return "completed"
        if value_str.isdigit():
            return int(value_str)
        return None
    except Exception as e:
        logger.error(f"Error reading A1 => {e}")
        return None

def update_cell(sheet, row, col, value):
    """
    Wrapper for updating a single cell using the retry logic.
    """
    return retry_google_sheets_operation(sheet.update_cell, row, col, value)

def update_range(sheet, cell_range, values):
    """
    Wrapper for updating a range of cells using the retry logic.
    """
    return retry_google_sheets_operation(sheet.update, cell_range, values)

# ---------------------------------------------------
# 5. MAIN LOGIC
# ---------------------------------------------------
def main():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    # Initialize a global variable to track the last pricing check time
    last_price_check_time = None

    try:
        # Connect to the worksheet
        client = gspread.authorize(creds)
        sheet = client.open('Amazon API 2.1').worksheet('First Checks')
        logger.info("Successfully connected to Google Sheet.")

        # Determine start row from A1
        a1_val = check_a1_status(sheet)
        if a1_val is None or (isinstance(a1_val, str) and a1_val.lower() == "completed"):
            logger.info("A1 => Completed or invalid => default row 2 or exit.")
            if isinstance(a1_val, str):
                # A1 says "completed"
                exit(0)
            start_row = 2
        elif isinstance(a1_val, int):
            start_row = a1_val
            logger.info(f"Row index set to {start_row} based on A1.")
        else:
            start_row = 2
            logger.info("Invalid or empty A1 => default row 2.")

        # Determine how many rows to process
        total_rows = max(len(sheet.col_values(1)), len(sheet.col_values(2)))
        if total_rows == 0:
            logger.info("No rows found => exiting.")
            total_rows = 1

        row_index = start_row
        logger.info(f"Will process rows {row_index} to {total_rows + 1}...")

        while row_index <= total_rows:
            try:
                access_token = get_access_token()

                # If Column BA (53) is set, skip
                col_ba_val = retry_google_sheets_operation(sheet.cell, row_index, 53).value
                if col_ba_val:
                    logger.info(f"Row {row_index}: BA => '{col_ba_val}', skipping.")
                    a1_val = check_a1_status(sheet)
                    if a1_val == "completed":
                        logger.info("A1 => Completed => stop.")
                        exit(0)
                    elif isinstance(a1_val, int):
                        row_index = a1_val
                    else:
                        row_index = 2
                    continue

                # Barcode
                barcode = retry_google_sheets_operation(sheet.cell, row_index, 2).value
                if not barcode:
                    logger.info(f"Row {row_index}: No barcode => skip.")
                    a1_val = check_a1_status(sheet)
                    if a1_val == "completed":
                        logger.info("A1 => Completed => exit.")
                        exit(0)
                    elif isinstance(a1_val, int):
                        row_index = a1_val
                    else:
                        row_index = 2
                    continue

                logger.info(f"Processing row {row_index}, barcode => {barcode}")

                # 1) Catalog details
                details = get_catalog_details(barcode, access_token)
                if not details or "asin" not in details:
                    logger.info(f"No valid ASIN => row {row_index} => marking NOASIN.")
                    update_cell(sheet, row_index, 53, "NOASIN")
                    row_index += 1
                    continue

                asin = details["asin"]
                brand_name = details.get("brand", "Unknown")
                rank = details.get("rank", float('inf'))
                release_date = details.get("release_date", "N/A")

                logger.info(f"ASIN => {asin}, Brand => {brand_name}, Rank => {rank}, Release => {release_date}")

                # 2) Rank check
                if rank > 50000:
                    logger.info(f"Rank {rank} > 50000 => row {row_index} => marking OVER50K.")
                    update_cell(sheet, row_index, 53, "OVER50K")
                    row_index += 1
                    continue

                # 3) Fill basic info => F..M
                dims = details.get("dimensions", {})
                height = round(dims.get("height", {}).get("value", 0) * 25.4)
                width = round(dims.get("width", {}).get("value", 0) * 25.4)
                length = round(dims.get("length", {}).get("value", 0) * 25.4)
                weight_lbs = details.get("weight", 0)
                if isinstance(weight_lbs, (int, float)):
                    weight_grams = round(weight_lbs * 453.592, 2)
                else:
                    weight_grams = 0

                update_range(sheet, f"F{row_index}:M{row_index}", [[
                    asin,
                    rank,
                    release_date,
                    brand_name,
                    height,
                    width,
                    length,
                    weight_grams
                ]])

                # 4) Hazmat
                hazmat_elig = check_eligibility_for_asin(asin, access_token)
                if not hazmat_elig or not hazmat_elig.get("eligible"):
                    logger.info(f"Row {row_index}: Hazmat => marking HAZMATFAIL.")
                    update_cell(sheet, row_index, 53, "HAZMATFAIL")
                    row_index += 1
                    continue
                else:
                    update_cell(sheet, row_index, 14, "Yes")
                    logger.info(f"Row {row_index}: Hazmat OK => brand & ROI next.")

                # 5) Check Product Cost
                cost_cell_raw = (retry_google_sheets_operation(sheet.cell, row_index, 3).value or "").strip()
                vat_cell_raw = (retry_google_sheets_operation(sheet.cell, row_index, 4).value or "0").strip()

                # If cost is N/A, mark NOCOST and skip
                if cost_cell_raw.lower() == "n/a" or not cost_cell_raw:
                    logger.info(f"Row {row_index}: Cost is N/A or blank => marking NOCOST.")
                    update_cell(sheet, row_index, 53, "NOCOST")
                    row_index += 1
                    continue

                try:
                    product_cost = round(float(cost_cell_raw), 2)
                except ValueError:
                    logger.info(f"Row {row_index}: Unable to parse cost => marking NOCOST.")
                    update_cell(sheet, row_index, 53, "NOCOST")
                    row_index += 1
                    continue

                try:
                    vat_dec = float(vat_cell_raw) / 100.0
                except:
                    logger.info(f"Row {row_index}: Invalid VAT => defaulting to 0.")
                    vat_dec = 0.0

                # 6) Pricing
                current_time = time.time()
                if last_price_check_time is not None:
                    elapsed = current_time - last_price_check_time
                    wait_time = 30 - elapsed
                    if wait_time > 0:
                        logger.info(f"Waiting {wait_time:.2f} seconds before pricing check.")
                        time.sleep(wait_time)
                price_res = get_pricing_details_for_asin(asin, access_token)
                last_price_check_time = time.time()

                buy_box_str = price_res.get("buy_box_price", "N/A")
                lowest_afn_str = price_res.get("lowest_afn_price", "N/A")
                try:
                    bbp = float(buy_box_str) if buy_box_str != "N/A" else 0
                    laf = float(lowest_afn_str) if lowest_afn_str != "N/A" else 0
                except:
                    bbp = 0
                    laf = 0

                # Update columns P..Q
                update_range(sheet, f"P{row_index}:Q{row_index}", [[bbp, laf]])
                final_price = max(bbp, laf)
                logger.info(f"Buy Box => {bbp}, Lowest AFN => {laf}, Final => {final_price}")

                # 7) Fees
                fee_details = {}
                for fee_try in range(3):
                    fee_details = get_fees_estimate_for_asin(asin, final_price, access_token)
                    if "error" not in fee_details:
                        break
                    time.sleep(1)

                referral_fee = round(fee_details.get("referral_fee", 0), 2)
                fba_fee = round(fee_details.get("fba_fee", 0), 2)
                ref_pct = round((referral_fee / final_price) * 100 if final_price else 0, 0)
                digital_fee = round((fba_fee + referral_fee) * 0.02, 2)
                shipping_est = 0

                if weight_grams > 0:
                    shipping_est = round(weight_grams * 0.0002045, 2)

                vat_adjusted = round(bbp / (1 + vat_dec), 2) if bbp else 0
                total_costs = round(product_cost + fba_fee + digital_fee + shipping_est, 2) * 1.03
                break_even = round((total_costs * (1 + vat_dec)) * (1 + (ref_pct / 100)), 2)
                min_sell = round(break_even * 1.20, 2)

                # Update R..Z
                update_range(sheet, f"R{row_index}:Z{row_index}", [[
                    "N/A", bbp, fba_fee, referral_fee, digital_fee,
                    shipping_est, vat_adjusted, break_even, min_sell
                ]])
                logger.info(
                    f"Row {row_index}: break_even={break_even}, min_sell={min_sell}, final_price={final_price}"
                )

                # ROI check
                if min_sell > final_price and final_price > 0:
                    logger.info(f"Row {row_index}: ROI check => marking ROIFAIL.")
                    update_cell(sheet, row_index, 53, "ROIFAIL")
                    row_index += 1
                    continue

                # 9) DETERMINE IF WE ALREADY HAVE A REAL DATE IN COLUMN H
                raw_h = (retry_google_sheets_operation(sheet.cell, row_index, 8).value or "").strip()
                h_lower = raw_h.lower()
                logger.info(f"Row {row_index}: Column H => '{raw_h}' (lower='{h_lower}')")

                if h_lower == "" or h_lower == "n/a":
                    # => no date => do normal date scraping
                    logger.info(
                        f"Row {row_index}: No date in H => We'll try new Chrome, then fallback if points >=2.5."
                    )
                    point_score_val = 0.0
                    try:
                        point_score_raw = retry_google_sheets_operation(sheet.cell, row_index, 34).value or "0"
                        point_score_val = float(point_score_raw)
                    except:
                        pass
                    old_chrome_forced = (point_score_val >= 2.5)
                    logger.info(f"Row {row_index}: Points => {point_score_val}, oldChrome? {old_chrome_forced}")

                    scraped = process_passed_product(
                        asin=asin,
                        break_even_price=break_even,
                        min_sell_price=min_sell,
                        product_cost=product_cost,
                        row_index=row_index,
                        brand_name=brand_name,
                        vat_rate=vat_dec * 100,
                        skip_date_scraping=False,
                        old_chrome_forced=old_chrome_forced,
                        bbp_driver=bbp_driver,
                        date_driver=legacy_driver
                    )

                    if scraped.get("success"):
                        data = scraped["scraped_data"]

                        # ADDED: update break even in column Y (25) if new value is > 0
                        bbp_break_even = data.get("updated_break_even", 0)
                        if bbp_break_even > 0:
                            logger.info(f"Row {row_index}: Updating break even => {bbp_break_even}")
                            update_cell(sheet, row_index, 25, round(bbp_break_even, 2))

                        final_date = data.get("product_info", "N/A")
                        logger.info(f"Row {row_index}: final_date => {final_date}")

                        if final_date == "N/A":
                            update_cell(sheet, row_index, 53, "NODATE")
                        else:
                            update_cell(sheet, row_index, 53, "DATEFOUND")

                        sc_date = data.get("scan_date", "")
                        title = data.get("main_title", "")
                        msold = data.get("monthly_sold", "")
                        rating = data.get("rating", "")
                        var_reviews = data.get("variant_reviews", "")
                        reviews_text = data.get("reviews_text", "")

                        try:
                            update_range(
                                sheet,
                                f"AA{row_index}:AG{row_index}",
                                [[sc_date, title, msold, rating, final_date, var_reviews, reviews_text]]
                            )
                        except Exception as e:
                            logger.error(f"Row {row_index}: Error updating columns => {e}")
                            update_cell(sheet, row_index, 53, "NODATE")

                        formula_ah = (
                            f"=iferror(if(VALUE(AF{row_index})>((today()-AE{row_index})/365)*40,"
                            f"if(VALUE(AF{row_index})>((today()-AE{row_index})/365)*50,1,0.5),0),0) + "
                            f"iferror(if(VALUE(AD{row_index})>=3.5,if(VALUE(AD{row_index})>=4,1,0.5),0),0) + "
                            f"iferror(if(VALUE(AG{row_index})>=6,1,0),0) + "
                            f"iferror(if(VALUE(AF{row_index})>=6,1,0),0) + 1"
                        )
                        try:
                            update_cell(sheet, row_index, 34, formula_ah)
                            logger.info(f"Row {row_index}: Wrote formula to AH => {formula_ah}")
                        except Exception as e:
                            logger.error(f"Row {row_index}: Error writing formula to AH => {e}")

                        formula_ba = f"=if(AH{row_index}>=3.5,\"PASS\",\"FAIL\")"
                        try:
                            update_cell(sheet, row_index, 53, formula_ba)
                            logger.info(f"Row {row_index}: Wrote formula to BA => {formula_ba}")
                        except Exception as e:
                            logger.error(f"Row {row_index}: Error writing formula to BA => {e}")

                    else:
                        err_reason = scraped.get("error", "")
                        logger.info(f"Row {row_index}: webscrape error => {err_reason}")

                        if err_reason == "ROI < 20%":
                            update_cell(sheet, row_index, 53, "LOWROI")
                            logger.warning(f"Row {row_index}: ROI < 20% — marked LOWROI")
                        elif err_reason == "CHROMEVERSIONFAIL":
                            update_cell(sheet, row_index, 53, "RESCAN")
                            logger.warning(f"Row {row_index}: Chrome mismatch — marked RESCAN")
                        elif err_reason == "Seller ~ brand":
                            update_cell(sheet, row_index, 53, "BRANDFAIL")
                            logger.warning(f"Row {row_index}: Brand matched seller — marked BRANDFAIL")
                        elif err_reason == "NODATE_OLDCHROME":
                            update_cell(sheet, row_index, 53, "NODATE")
                            logger.warning(f"Row {row_index}: No date found — marked NODATE")
                        elif err_reason == "REVIEWS_NO_UK":
                            update_cell(sheet, row_index, 53, "REVIEWFAIL")
                            logger.warning(f"Row {row_index}: No UK reviews — marked REVIEWFAIL")
                        else:
                            update_cell(sheet, row_index, 53, "SCRAPEFAIL")
                            logger.warning(f"Row {row_index}: Unknown scrape error — marked SCRAPEFAIL")


                else:
                    # => We have an actual date in H => skip date scraping
                    logger.info(f"Row {row_index}: Column H => '{raw_h}', skip date scraping.")
                    point_score_val = 0.0
                    try:
                        point_score_raw = retry_google_sheets_operation(sheet.cell, row_index, 34).value or "0"
                        point_score_val = float(point_score_raw)
                    except:
                        pass

                    old_chrome_forced = (point_score_val >= 2.5)
                    logger.info(
                        f"Row {row_index}: Because H has a real date, skip_date_scraping=True, oldChrome? => {old_chrome_forced}"
                    )

                    scraped = process_passed_product(
                        asin=asin,
                        break_even_price=break_even,
                        min_sell_price=min_sell,
                        product_cost=product_cost,
                        row_index=row_index,
                        brand_name=brand_name,
                        vat_rate=vat_dec * 100,
                        skip_date_scraping=True,
                        old_chrome_forced=old_chrome_forced,
                        bbp_driver=bbp_driver,
                        date_driver=legacy_driver
                    )


                    if "scraped_data" in scraped:
                        # Overwrite product_info with the date from H
                        scraped["scraped_data"]["product_info"] = raw_h

                    if scraped.get("success"):
                        data = scraped["scraped_data"]

                        # ADDED: update break even in column Y if new value is > 0
                        bbp_break_even = data.get("updated_break_even", 0)
                        if bbp_break_even > 0:
                            logger.info(f"Row {row_index}: Updating break even => {bbp_break_even}")
                            update_cell(sheet, row_index, 25, round(bbp_break_even, 2))

                        final_date = data.get("product_info", "N/A")
                        logger.info(f"Row {row_index}: Using pre-stored date => {final_date}")
                        update_cell(sheet, row_index, 53, "DATEFOUND")

                        sc_date = data.get("scan_date", "")
                        title = data.get("main_title", "")
                        msold = data.get("monthly_sold", "")
                        rating = data.get("rating", "")
                        var_reviews = data.get("variant_reviews", "")
                        reviews_text = data.get("reviews_text", "")

                        try:
                            update_range(
                                sheet,
                                f"AA{row_index}:AG{row_index}",
                                [[sc_date, title, msold, rating, final_date, var_reviews, reviews_text]]
                            )
                        except Exception as e:
                            logger.info(f"Row {row_index}: Error updating columns => {e}")
                            update_cell(sheet, row_index, 53, "NODATE")

                        formula_ah = (
                            f"=iferror(if(VALUE(AF{row_index})>((today()-AE{row_index})/365)*40,"
                            f"if(VALUE(AF{row_index})>((today()-AE{row_index})/365)*50,1,0.5),0),0) + "
                            f"iferror(if(VALUE(AD{row_index})>=3.5,if(VALUE(AD{row_index})>=4,1,0.5),0),0) + "
                            f"iferror(if(VALUE(AG{row_index})>=6,1,0),0) + "
                            f"iferror(if(VALUE(AF{row_index})>=6,1,0),0) + 1"
                        )
                        try:
                            update_cell(sheet, row_index, 34, formula_ah)
                            logger.info(f"Row {row_index}: Wrote formula to AH => {formula_ah}")
                        except Exception as e:
                            logger.error(f"Row {row_index}: Error writing formula to AH => {e}")

                        formula_ba = f"=if(AH{row_index}>=3.5,\"PASS\",\"FAIL\")"
                        try:
                            update_cell(sheet, row_index, 53, formula_ba)
                            logger.info(f"Row {row_index}: Wrote formula to BA => {formula_ba}")
                        except Exception as e:
                            logger.error(f"Row {row_index}: Error writing formula to BA => {e}")

                    else:
                        err_reason = scraped.get("error", "")
                        logger.info(f"Row {row_index} => webscrape error => {err_reason}")
                        if err_reason == "ROI < 20%":
                            update_cell(sheet, row_index, 53, "LOWROI")
                        elif err_reason == "Seller ~ brand":
                            update_cell(sheet, row_index, 53, "BRANDFAIL")
                        elif err_reason == "NODATE_OLDCHROME":
                            update_cell(sheet, row_index, 53, "NODATE")
                        elif err_reason == "REVIEWS_NO_UK":
                            update_cell(sheet, row_index, 53, "REVIEWFAIL")
                        else:
                            update_cell(sheet, row_index, 53, "RESCAN")

            except Exception as e:
                logger.error(f"Unhandled exception row {row_index}: {e}")
                a1_val = check_a1_status(sheet)
                if a1_val == "completed":
                    logger.info("A1 => completed => stop")
                    exit(0)
                if isinstance(a1_val, int):
                    row_index = a1_val
                else:
                    row_index = 2

            # Check A1 again at the end of each loop
            v = check_a1_status(sheet)
            if v == "completed":
                logger.info("A1 => completed => stop")
                exit(0)
            elif isinstance(v, int):
                row_index = v
            else:
                row_index = row_index + 1

    except Exception as e:
        logger.error(f"Error accessing Google Sheet => {e}")

# 6. ENTRY POINT — Launch Persistent Chrome
# ---------------------------------------------------
import undetected_chromedriver as uc
from selenium import webdriver

def launch_bbp_driver():
    options = uc.ChromeOptions()
    options.binary_location = r"C:\Chrome_UC136\bin\chrome.exe"
    options.add_argument(r"--user-data-dir=C:\Users\Luke\AppData\Local\Chrome_UC136")
    options.add_argument(r"--profile-directory=BBPProfile")
    driver = uc.Chrome(options=options)
    driver.set_window_position(0, 0)
    driver.set_window_size(1280, 720)
    return driver

def launch_legacy_driver():
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options

    options = Options()
    options.binary_location = r"C:\Users\Luke\PortableApps\GoogleChromePortable\App\Chrome-bin\chrome.exe"
    options.add_argument(r"--user-data-dir=C:\Users\Luke\AppData\Local\Chrome_91")
    options.add_argument(r"--profile-directory=Profile 1")

    service = Service(r"C:\Users\Luke\PortableApps\GoogleChromePortable\App\Chrome-bin\chromedriver.exe")
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_window_position(1280, 0)
    driver.set_window_size(1280, 720)
    return driver


def patch_chrome_drivers(bbp, legacy):
    uc.Chrome = lambda *args, **kwargs: bbp
    webdriver.Chrome = lambda *args, **kwargs: legacy

if __name__ == "__main__":
    print("[INFO] Launching Chrome in 5 seconds... switch to Webscraper desktop now.")
    time.sleep(5)
    bbp_driver = launch_bbp_driver()
    legacy_driver = launch_legacy_driver()
    patch_chrome_drivers(bbp_driver, legacy_driver)

    try:
        main()
    finally:
        print("[INFO] Closing Chrome browsers.")
        bbp_driver.quit()
        legacy_driver.quit()

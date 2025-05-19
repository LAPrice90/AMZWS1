# Webscrape.py

# ---------------------------------------------------
# 1. IMPORTS
# ---------------------------------------------------
# Standard libraries
import time
import random
import logging
from difflib import SequenceMatcher

# Third-party libraries
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup

# Local modules
from WebscraperS2 import scrape_main_page, extract_date

# ---------------------------------------------------
# 2. LOGGER SETUP
# ---------------------------------------------------
logger = logging.getLogger("Webscrape Bybot")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------------------
# 3. HELPER FUNCTIONS
# ---------------------------------------------------
def handle_overlays(driver):
    """
    Attempts to close/hide any pop-up overlays on the page.
    """
    try:
        overlay = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "newUserDiv"))
        )
        driver.execute_script("arguments[0].style.display = 'none';", overlay)
        logger.info("Overlay hidden.")
    except:
        logger.info("No overlay or ignoring.")


def clean_seller_name(raw_name):
    """
    Strips HTML tags, removes prefixes like 'FBA Prime', 'MF Prime', etc.
    and returns a clean seller name.
    """
    try:
        soup = BeautifulSoup(raw_name, "html.parser")
        text = soup.get_text(strip=True)
        for prefix in ["FBA Prime", "MF Prime"]:
            if text.startswith(prefix):
                text = text[len(prefix):].strip()
        return text
    except:
        return raw_name


def is_similar(brand_name, seller_name, threshold=0.8):
    """
    Returns True if brand_name and seller_name are at least 'threshold' similar.
    """
    ratio = SequenceMatcher(None, brand_name.lower(), clean_seller_name(seller_name).lower()).ratio()
    logger.info(f"similar => {brand_name} vs {seller_name}, ratio => {ratio:.2f}")
    return ratio >= threshold


def login_to_buybotpro(driver, email, password):
    """
    Logs into BuyBotPro by locating email/password fields and the login button.
    """
    try:
        email_f = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#loginEmail"))
        )
        pass_f = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#loginPassword"))
        )
        login_b = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#loginBtn"))
        )

        email_f.clear()
        email_f.send_keys(email)
        pass_f.clear()
        pass_f.send_keys(password)
        login_b.click()
        logger.info("Submitted BBP login.")
        time.sleep(5)
    except Exception as e:
        logger.info(f"BBP login error => {e}")


def update_vat_rate(driver, vat_rate):
    """
    Forces the VAT rate on BuyBotPro's interface every time,
    ensuring we don't rely on any existing or default values.
    """
    try:
        vat_f = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="txtVatRate"]'))
        )
        logger.info(f"Forcing VAT => {vat_rate}%")
        vat_f.clear()
        vat_f.send_keys(str(vat_rate))  # e.g. "20" or "0"
        time.sleep(2)  # let BBP recalc
        logger.info(f"Set VAT => {vat_rate}% complete.")
    except Exception as e:
        logger.error(f"update_vat_rate => {e}")


def validate_scraped_data(data):
    """
    Ensures the returned dictionary has all required keys.
    Missing keys are set to 'N/A'.
    """
    req = [
        "scan_date",
        "main_title",
        "monthly_sold",
        "rating",
        "product_info",   # Usually release date or product details
        "variant_reviews",
        "reviews_text",
        "historical_uk_reviews",
    ]
    return {k: data.get(k, "N/A") for k in req}


def fallback_scrape_date_with_driver(driver, asin):
    """
    Reuse an already-open Chrome 91 driver to scrape the product info section and extract release date.
    """
    try:
        if driver is None:
            logger.error("[Chrome91] No driver provided to fallback_scrape_date_with_driver.")
            return "N/A"

        url = f"https://www.amazon.co.uk/dp/{asin}"
        logger.info(f"[Chrome91] Navigating => {url}")
        driver.get(url)
        time.sleep(2)

        # ✅ Accept cookies if the button is present
        try:
            cookie_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="sp-cc-accept"]'))
            )
            cookie_button.click()
            logger.info("[Chrome91] Accepted cookies popup.")
        except Exception as e:
            logger.info(f"[Chrome91] No cookie accept popup found or not clickable => {e}")

        driver.delete_all_cookies()
        dist = random.randint(100, 500)
        driver.execute_script(f"window.scrollBy(0,{dist})")
        time.sleep(2)

        product_info_text = "N/A"

        # Primary XPath
        try:
            product_info_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="productDetailsWithModules_feature_div"]'))
            )
            product_info_text = product_info_element.text.strip()
            logger.info("[Chrome91] Product info extracted using primary XPath.")
        except Exception:
            logger.info("[Chrome91] Primary XPath failed. Attempting fallback XPath for product info...")
            try:
                product_info_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="productDetails_feature_div"]'))
                )
                product_info_text = product_info_element.text.strip()
                logger.info("[Chrome91] Product info extracted using fallback XPath.")
            except Exception:
                logger.error("[Chrome91] Both primary and fallback XPaths failed. Product info not found.")
                product_info_text = "N/A"

        # Second fallback
        if product_info_text == "N/A":
            try:
                product_info_element = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="detailBullets_feature_div"]'))
                )
                product_info_text = product_info_element.text.strip()
                logger.info("[Chrome91] Product info extracted using second fallback XPath.")
            except:
                logger.error("[Chrome91] Second fallback also failed for product info.")

        logger.info(f"[Chrome91] Full product info text:\n{product_info_text}")

        extracted = extract_date(product_info_text)
        if not extracted:
            if "Sept." in product_info_text:
                logger.info("[Chrome91] Detected 'Sept.' => attempting fallback parse by replacing 'Sept.' with 'Sep.'")
                replaced_text = product_info_text.replace("Sept.", "Sep.")
                extracted = extract_date(replaced_text)
                if extracted:
                    logger.info(f"[Chrome91] extracted_date after 'Sept.' replacement => {extracted}")

        if extracted:
            logger.info(f"[Chrome91] extracted_date => {extracted}")
            return extracted
        else:
            logger.warning("[Chrome91] No date found in product info text => returning N/A")
            return "N/A"

    except Exception as e:
        logger.error(f"[Chrome91] error => {e}")
        return "N/A"


# ---------------------------------------------------
# 4. MAIN PIPELINE FUNCTION
# ---------------------------------------------------
def process_passed_product(
    asin,
    break_even_price,
    min_sell_price,
    product_cost,
    row_index,
    brand_name,
    vat_rate,
    skip_date_scraping=False,
    old_chrome_forced=False,
    bbp_driver=None,
    date_driver=None
):
    """
    Main pipeline for scraping.
    Launches modern Chrome by default, optionally uses fallback Chrome 91,
    sets cost & VAT in BuyBotPro, then calculates a realistic Sell Price
    from historical data to override the BBP auto-filled price if needed.
    """
    new_chrome_exe = r"C:\Chrome_UC136\bin\chrome.exe"

    if old_chrome_forced and not skip_date_scraping:
        logger.info("User requested old Chrome approach (score >= 2.5).")
        use_old_for_date = True
    else:
        use_old_for_date = False

    options = uc.ChromeOptions()
    options.binary_location = r"C:\Chrome_UC136\bin\chrome.exe"
    options.add_argument(r"--user-data-dir=C:\Users\Luke\AppData\Local\Chrome_UC136")
    options.add_argument(r"--profile-directory=BBPProfile")


    options.add_argument("--flag-switches-begin")
    options.add_argument("--flag-switches-end")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--allow-running-insecure-content")
    options.add_argument("--disable-infobars")
    options.add_argument("--remote-debugging-port=9222")

    try:
        if not bbp_driver:
            logger.error("❌ No bbp_driver passed to process_passed_product.")
            return {"success": False, "scraped_data": {}, "error": "No BBP driver provided"}
        
        driver = bbp_driver
        logger.info("[BBP] Using existing BBP Chrome driver passed from firstCheck.py")


        url = f"https://www.amazon.co.uk/dp/{asin}"
        logger.info(f"[Profile5] Going => {url}")
        driver.get(url)
        time.sleep(3)
        driver.refresh()
        time.sleep(3)

        try:
            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.ID, "bbp-container"))
            )
            logger.info("[Profile5] BBP iframe detected after refresh.")
        except Exception as e:
            logger.warning(f"[Profile5] BBP iframe still not found after refresh => {e}")



        # Hide overlays
        handle_overlays(driver)

        # Random scrolling
        for _ in range(2):
            dist = random.randint(100, 500)
            driver.execute_script(f"window.scrollBy(0,{dist})")
            time.sleep(1)

        # Attempt BBP iframe
        try:
            iframe = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "bbp-frame"))
            )
            driver.switch_to.frame(iframe)
            logger.info("[Profile5] Found BBP iframe.")

            login_to_buybotpro(driver, "dan@drjhardware.co.uk", "Systembox-60811963")
            time.sleep(2)

            try:
                # ---------------------------
                # 1) ENTER COST + UPDATE VAT
                # ---------------------------
                cost_f = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#txtBuyPrice"))
                )
                cost_f.clear()
                cost_f.send_keys(f"{product_cost:.2f}")
                logger.info(f"[Profile5] cost => {product_cost:.2f}")

                time.sleep(2)
                update_vat_rate(driver, vat_rate)
                time.sleep(2)

                # ---------------------------
                # 2) PARSE HISTORICAL PRICE DATA
                # ---------------------------
                hist_table = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#asinAverageStatisticsDataTable"))
                )
                rows = hist_table.find_elements(By.TAG_NAME, "tr")
                hist_data = {}
                for r in rows:
                    tds = r.find_elements(By.TAG_NAME, "td")
                    if len(tds) >= 5:
                        key = tds[0].text.strip()
                        vals = []
                        for c in tds[1:]:
                            raw = c.text.strip()
                            if raw == "-" or not raw:
                                vals.append(0.0)
                            else:
                                try:
                                    vals.append(float(raw))
                                except:
                                    vals.append(0.0)
                        hist_data[key] = vals

                logger.info("[Profile5] Price hist =>")
                for k, v in hist_data.items():
                    logger.info(f"{k}: {v}")

                # We'll define a helper to get the best "smart" average
                def pick_smart_price():
                    """
                    Tries 30d => 90d => 180d for Amazon / FBA, then fallback to BuyBox
                    Returns float or 0 if no data.
                    """
                    amazon_list = hist_data.get("Amazon (£)", [])
                    fba_list = hist_data.get("FBA (£)", [])
                    box_list = hist_data.get("BuyBox (£)", [])

                    def val_or_zero(arr, idx):
                        try:
                            return float(arr[idx])
                        except:
                            return 0.0

                    # We'll define a step approach: 30-day => index=1, 90-day => index=2, 180 => 3
                    # 1) lowest of amazon/fba at 30d
                    a30 = val_or_zero(amazon_list, 1)
                    f30 = val_or_zero(fba_list, 1)
                    if a30 > 0 or f30 > 0:
                        base_30 = 0
                        if a30 > 0 and f30 > 0:
                            base_30 = min(a30, f30)
                        elif a30 > 0:
                            base_30 = a30
                        else:
                            base_30 = f30
                        if base_30 > 0:
                            return base_30

                    # 2) buy box 30
                    bb30 = val_or_zero(box_list, 1)
                    if bb30 > 0:
                        return bb30

                    # 3) amazon/fba 90
                    a90 = val_or_zero(amazon_list, 2)
                    f90 = val_or_zero(fba_list, 2)
                    if a90 > 0 or f90 > 0:
                        base_90 = 0
                        if a90 > 0 and f90 > 0:
                            base_90 = min(a90, f90)
                        elif a90 > 0:
                            base_90 = a90
                        else:
                            base_90 = f90
                        if base_90 > 0:
                            return base_90

                    # 4) buy box 90
                    bb90 = val_or_zero(box_list, 2)
                    if bb90 > 0:
                        return bb90

                    # 5) amazon/fba 180
                    a180 = val_or_zero(amazon_list, 3)
                    f180 = val_or_zero(fba_list, 3)
                    if a180 > 0 or f180 > 0:
                        base_180 = 0
                        if a180 > 0 and f180 > 0:
                            base_180 = min(a180, f180)
                        elif a180 > 0:
                            base_180 = a180
                        else:
                            base_180 = f180
                        if base_180 > 0:
                            return base_180

                    # 6) buy box 180
                    bb180 = val_or_zero(box_list, 3)
                    if bb180 > 0:
                        return bb180

                    return 0.0

                smart_price = pick_smart_price()
                logger.info(
                    f"[Profile5] smart_price => {smart_price:.2f}" if smart_price else "No valid historical average found."
                )

                # brand check
                try:
                    prime_check = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "#primeOnly"))
                    )
                    if not prime_check.is_selected():
                        ActionChains(driver).move_to_element(prime_check).click().perform()
                        time.sleep(2)
                        logger.info("Filtered prime only.")
                except Exception as e:
                    logger.info(f"Error prime check => {e}")

                sellers = []
                for i in range(1, 4):
                    try:
                        sl = WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located(
                                (By.CSS_SELECTOR, f"#competitionAnalysisDataTable > tbody > tr:nth-child({i}) > td:nth-child(1) > a")
                            )
                        )
                        sn = sl.get_attribute("data-original-title").strip()
                        sellers.append(sn)
                        logger.info(f"Seller {i} => {sn}")
                    except:
                        pass

                for s in sellers:
                    if is_similar(brand_name, s):
                        logger.info(f"Seller {s} ~ brand => fail.")
                        return {"success": False, "scraped_data": {}, "error": "Seller ~ brand"}

                logger.info("[Profile5] brand check done => now let's set final Sell Price if needed.")

                # ---------------------------
                # 3) COMPARE + OVERRIDE PRICE
                # ---------------------------
                # Get the auto-filled price from the "calculatorSellPrice" field
                try:
                    auto_fill_el = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="calculatorSellPrice"]'))
                    )
                    current_auto_price_str = auto_fill_el.get_attribute("value").strip()
                    try:
                        current_auto_price = float(current_auto_price_str)
                    except:
                        current_auto_price = 999999.0  # fallback if unreadable
                except Exception as e:
                    logger.warning(f"Could not read auto-filled Sell Price => {e}")
                    current_auto_price = 999999.0

                if smart_price > 0:
                    # We'll choose finalPrice as the lowest of the two
                    final_price = min(smart_price, current_auto_price)
                    if final_price < current_auto_price:
                        logger.info(f"Overriding Sell Price => from {current_auto_price} down to {final_price}")
                        try:
                            auto_fill_el.clear()
                            auto_fill_el.send_keys(f"{final_price:.2f}")
                            time.sleep(2)  # wait for BBP to recalc ROI
                        except Exception as e:
                            logger.warning(f"Could not override Sell Price => {e}")
                    else:
                        logger.info("Keeping BBP's auto-filled price.")
                else:
                    logger.info("No valid historical price => no override. (BBP's price stands)")

                # ---------------------------
                # 4) READ ROI AFTER FINAL PRICE
                # ---------------------------
                try:
                    roi_el = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "#quickInfoRoi"))
                    )
                    roi_str = roi_el.text.strip("%")
                    logger.info(f"ROI => {roi_str}%")
                    try:
                        roi_val = float(roi_str)
                    except:
                        roi_val = 0
                    if roi_val < 20:
                        logger.info("ROI < 20 => fail.")
                        return {"success": False, "scraped_data": {}, "error": "ROI < 20%"}
                except Exception as e:
                    logger.warning(f"Could not read ROI after final price => {e}")
                    # fallback: if ROI can't be read, we proceed but it's suspicious

                logger.info("[Profile5] done brand+ROI => now main page scraping.")

            except Exception as e:
                logger.error(f"BuyBotPro checks => {e}")
                return {"success": False, "scraped_data": {}, "error": "BuyBotPro error"}

        except Exception as e:
            logger.error(f"No BBP iframe => {e}")
            return {"success": False, "scraped_data": {}, "error": "No BBP iframe"}

        # Switch back from the BBP iframe
        driver.switch_to.default_content()

        # If we skip date scraping, just scrape partial info
        if skip_date_scraping:
            logger.info("Skipping date scraping, but still reading main page for reviews, monthly sold, etc.")
            temp_data = scrape_main_page(driver)
            temp_data["product_info"] = "N/A"
            data = temp_data
        else:
            logger.info("Normal date scraping with new Chrome. If forced old Chrome, will do after this step for date.")
            data = scrape_main_page(driver)

        validated = validate_scraped_data(data)

        # Placeholder for break-even value (not implemented yet)
        validated["updated_break_even"] = 0

        found_date = validated.get("product_info", "N/A")

        # If forced old Chrome for date scraping
        if use_old_for_date and not skip_date_scraping:
            old_date = fallback_scrape_date_with_driver(date_driver, asin)
            if old_date != "N/A":
                validated["product_info"] = old_date
            else:
                logger.info("[Chrome91] no date => NODATE_OLDCHROME.")
                return {"success": False, "scraped_data": validated, "error": "NODATE_OLDCHROME"}

        # Historical UK review check
        historical_uk_str = validated.get("historical_uk_reviews", "0")
        try:
            historical_uk_val = int(historical_uk_str)
        except:
            historical_uk_val = 0

        if historical_uk_val == 0:
            logger.info("No historical UK reviews => fail.")
            return {"success": False, "scraped_data": validated, "error": "REVIEWS_NO_UK"}

        # If we want to double-check date scraping
        if (not skip_date_scraping) and (not use_old_for_date):
            if found_date == "N/A":
                logger.info("[Profile5] date is N/A => fallback to Chrome91.")

                old_date = fallback_scrape_date_with_driver(date_driver, asin)
                if old_date == "N/A":
                    logger.info("[Chrome91] no date => NODATE_OLDCHROME.")
                    return {"success": False, "scraped_data": validated, "error": "NODATE_OLDCHROME"}
                else:
                    validated["product_info"] = old_date

        return {"success": True, "scraped_data": validated}

    except Exception as e:
        logger.error(f"unexpected => {e}")
        return {"success": False, "scraped_data": {}, "error": str(e)}

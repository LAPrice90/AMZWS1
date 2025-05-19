# WebscraperS2.py

# ---------------------------------------------------
# 1. IMPORTS
# ---------------------------------------------------
import re
import random
import time
import logging
from datetime import datetime, timedelta

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from dropdownSelector import test_variant_dropdown

logger = logging.getLogger("Webscraper S2")

# ---------------------------------------------------
# 2. HELPER FUNCTIONS
# ---------------------------------------------------
def extract_3_month_uk_reviews(review_text):
    """
    Returns the count of UK reviews within the last 3 months.
    If NO UK reviews are found, returns "No UK".
    """
    today = datetime.now()
    three_months_ago = today - timedelta(days=90)

    pattern = re.compile(r"Reviewed in the United Kingdom on (\d{1,2} [A-Za-z]+ \d{4})", re.IGNORECASE)
    has_any_uk = False
    recent_count = 0

    for match in pattern.finditer(review_text):
        has_any_uk = True
        date_str = match.group(1)
        # parse date
        try:
            review_date = datetime.strptime(date_str, "%d %B %Y")
            if review_date >= three_months_ago:
                recent_count += 1
        except:
            pass

    if not has_any_uk:
        return "No UK"
    return recent_count

def extract_historical_uk_reviews(review_text):
    """
    Returns the TOTAL count of UK reviews historically (any date).
    If none at all, returns 0.
    """
    pattern = re.compile(r"Reviewed in the United Kingdom on (\d{1,2} [A-Za-z]+ \d{4})", re.IGNORECASE)
    matches = pattern.findall(review_text)
    return len(matches)

def extract_date(product_info_text):
    """
    Extracts a date from the provided text using multiple regex patterns.
    Returns the date as YYYY-MM-DD (str) or None if not found.
    """
    date_patterns = [
        r"\b(\d{1,2} [A-Za-z]{3,9}\.? \d{4})\b",  # e.g., 5 Oct. 2011
        r"\b(\d{1,2}/\d{1,2}/\d{2,4})\b",         # e.g., 05/10/2011
        r"\b(\d{4}-\d{1,2}-\d{1,2})\b"            # e.g., 2011-10-05
    ]
    for pattern in date_patterns:
        match = re.search(pattern, product_info_text)
        if match:
            raw_date = match.group(1)
            try:
                return datetime.strptime(raw_date, "%d %b. %Y").strftime("%Y-%m-%d")
            except ValueError:
                try:
                    return datetime.strptime(raw_date, "%d %B %Y").strftime("%Y-%m-%d")
                except ValueError:
                    try:
                        return datetime.strptime(raw_date, "%d/%m/%Y").strftime("%Y-%m-%d")
                    except ValueError:
                        try:
                            return datetime.strptime(raw_date, "%Y-%m-%d").strftime("%Y-%m-%d")
                        except ValueError:
                            continue
    return None

def validate_scraped_data(data):
    """
    Validates scraped data to ensure all required keys are present.
    Missing keys are assigned 'N/A'.
    """
    required_keys = [
        "scan_date",
        "main_title",
        "monthly_sold",
        "rating",
        "product_info",
        "variant_reviews",
        "reviews_text",
        "historical_uk_reviews",
        "parent_total_reviews"  # <- ADDED to the required keys list
    ]
    return {k: data.get(k, "N/A") for k in required_keys}

def random_scroll(driver):
    """
    Performs random scrolling on the page to simulate user behavior.
    """
    import random
    scroll_height = driver.execute_script("return document.body.scrollHeight")
    for _ in range(random.randint(2, 5)):
        random_scroll_position = random.randint(100, scroll_height // 2)
        driver.execute_script(f"window.scrollTo(0, {random_scroll_position});")
        time.sleep(random.uniform(0.5, 1.5))

def find_dynamic_element(driver, locators, wait_time=10):
    """
    Attempts to find an element dynamically using multiple locator strategies.
    Returns the first WebElement found or None if no element is located.
    """
    from selenium.common.exceptions import TimeoutException
    for strategy, locator in locators:
        try:
            logger.info(f"Trying to locate element using {strategy}: {locator}")
            elem = WebDriverWait(driver, wait_time).until(
                EC.presence_of_element_located((strategy, locator))
            )
            if elem.is_displayed():
                logger.info(f"Element located using {strategy}: {locator}")
                return elem
        except TimeoutException:
            logger.warning(f"Timeout while trying {strategy}: {locator}")
        except Exception as e:
            logger.warning(f"Exception while trying {strategy}: {locator} => {e}")
    logger.error("Failed to locate element with all provided locators.")
    return None

# We'll import test_variant_dropdown from 'dropdownSelector'
from dropdownSelector import test_variant_dropdown

# ---------------------------------------------------
# MAIN SCRAPE FUNCTION
# ---------------------------------------------------
def scrape_main_page(driver):
    """
    Scrapes data from the main product page on Amazon using Selenium driver 
    that is already logged in. Returns a dictionary with:
      - scan_date
      - main_title
      - monthly_sold
      - rating
      - product_info        (the date or 'N/A')
      - variant_reviews
      - reviews_text        (3-month UK count)
      - historical_uk_reviews
      - parent_total_reviews
    """
    logger.info("Starting main page scraping with existing driver session.")

    try:
        # 1) Check if driver is valid
        if not driver.service.is_connectable():
            logger.error("Driver session is invalid. No scraping done.")
            return validate_scraped_data({})

        # 2) Basic data
        scan_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Scrape date: {scan_date}")

        # 3) Title
        try:
            title_el = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "productTitle"))
            )
            main_title = title_el.text.strip()
        except Exception as e:
            main_title = "N/A"
            logger.error(f"Error finding productTitle => {e}")

        # 4) socialProofingAsinFaceout => monthly sold
        try:
            sold_el = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="socialProofingAsinFaceout_feature_div"]'))
            )
            monthly_sold = sold_el.text.strip()
        except Exception as e:
            monthly_sold = "0"
            logger.error(f"Error scraping monthly_sold => {e}")

        # 5) rating
        try:
            rating_el = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="acrPopover"]/span[1]/a/span'))
            )
            rating_txt = rating_el.text.strip()
        except Exception as e:
            rating_txt = "N/A"
            logger.error(f"Error scraping rating => {e}")

        # 5.5) parent-level total reviews (from summary near title)
        try:
            parent_reviews_el = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="acrCustomerReviewText"]'))
            )
            parent_reviews_txt = parent_reviews_el.text.strip()
            match = re.search(r"([\d,]+)", parent_reviews_txt)
            if match:
                parent_total_reviews = match.group(1).replace(",", "")
            else:
                parent_total_reviews = "N/A"
        except Exception as e:
            parent_total_reviews = "N/A"
            logger.error(f"Error scraping parent total reviews => {e}")

        # 6) product info => date
        product_info_text = "N/A"
        try:
            details_el = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="productDetailsWithModules_feature_div"]'))
            )
            product_info_text = details_el.text.strip()
            logger.info("Found product details (modules).")
        except TimeoutException:
            logger.info("Primary XPATH for productDetailsWithModules_feature_div not found, fallback.")
            try:
                details_el = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="productDetails_feature_div"]'))
                )
                product_info_text = details_el.text.strip()
                logger.info("Found product info (fallback).")
            except Exception as e2:
                logger.error(f"No product info found => {e2}")
                product_info_text = "N/A"

        extracted_date = extract_date(product_info_text)
        if not extracted_date:
            extracted_date = "N/A"
            logger.info("No date found in product info.")

        # 7) random scroll
        random_scroll(driver)

        # 8) find reviews link
        reviews_link = find_dynamic_element(
            driver,
            [
                (By.XPATH, '//*[@id="reviews-medley-footer"]/div[2]/a'),
                (By.XPATH, '//*[@id="reviews-medley-footer"]/div/a'),
                (By.CSS_SELECTOR, '#reviews-medley-footer a')
            ],
            wait_time=10
        )
        if not reviews_link:
            logger.error("No reviews link found. Skipping reviews part.")
            # Build partial data
            partial = {
                "scan_date": scan_date,
                "main_title": main_title,
                "monthly_sold": monthly_sold,
                "rating": rating_txt,
                "product_info": extracted_date,
                "variant_reviews": "N/A",
                "reviews_text": "N/A",
                "historical_uk_reviews": "0",
                "parent_total_reviews": parent_total_reviews
            }
            return validate_scraped_data(partial)

        rev_url = reviews_link.get_attribute("href")
        if not rev_url:
            logger.error("Review link no href. Skipping reviews.")
            partial2 = {
                "scan_date": scan_date,
                "main_title": main_title,
                "monthly_sold": monthly_sold,
                "rating": rating_txt,
                "product_info": extracted_date,
                "variant_reviews": "N/A",
                "reviews_text": "N/A",
                "historical_uk_reviews": "0",
                "parent_total_reviews": parent_total_reviews
            }
            return validate_scraped_data(partial2)

        # navigate to reviews
        driver.get(rev_url)
        logger.info("Navigated to reviews page.")

        # wait load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="cm_cr-review_list"]'))
        )
        logger.info("reviews page loaded fully")

        # test variant dropdown
        test_variant_dropdown(driver, rev_url)
        logger.info("test_variant_dropdown done")

        # extract variant reviews
        variant_reviews = "N/A"
        try:
            variant_el = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="filter-info-section"]'))
            )
            raw_variant = variant_el.text.strip()
            logger.info(f"raw variant => {raw_variant}")
            match = re.search(r'(\d[\d,]*)\s+total ratings', raw_variant)
            if match:
                variant_reviews = match.group(1).replace(',', '')
        except TimeoutException:
            logger.error("variant reviews => timeout")

        # parse UK reviews
        reviews_text = "N/A"
        historical_uk_count = 0
        try:
            cm_rev = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="cm_cr-review_list"]'))
            )
            raw_rev_text = cm_rev.text

            # 3-month
            three_month_count = extract_3_month_uk_reviews(raw_rev_text)
            if three_month_count == "No UK":
                reviews_text = "No UK"
            else:
                reviews_text = str(three_month_count)

            # historical
            historical_uk_count = extract_historical_uk_reviews(raw_rev_text)
        except TimeoutException:
            logger.error("timeout cm_cr-review_list")
        except Exception as e:
            logger.error(f"err => {e}")

        scraped_data = {
            "scan_date": scan_date,
            "main_title": main_title,
            "monthly_sold": monthly_sold,
            "rating": rating_txt,
            "product_info": extracted_date,
            "variant_reviews": variant_reviews,
            "reviews_text": reviews_text,
            "historical_uk_reviews": str(historical_uk_count),
            "parent_total_reviews": parent_total_reviews
        }

        return validate_scraped_data(scraped_data)

    except Exception as e:
        logger.error(f"Fatal error => {e}")
        return validate_scraped_data({})
    finally:
        logger.info("WebscraperS2 => scrape_main_page done.")

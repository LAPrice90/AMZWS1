# main_page_scraper.py

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from datetime import datetime
from dropdownSelector import test_variant_dropdown
from datetime import datetime, timedelta
import random
import time
import logging
from selenium.common.exceptions import TimeoutException
import re
from datetime import datetime
import logging

logger = logging.getLogger("Webscraper S2")

def find_dynamic_element(driver, locators, wait_time=10):
    """
    Attempts to find an element dynamically using multiple locators.
    :param driver: Selenium WebDriver instance.
    :param locators: A list of tuples containing locator strategies and values (e.g., [(By.XPATH, "xpath1"), ...]).
    :param wait_time: Maximum wait time for each locator.
    :return: The first WebElement found or None if no element is found.
    """
    for strategy, locator in locators:
        try:
            logger.info(f"Trying to locate element using {strategy}: {locator}")
            element = WebDriverWait(driver, wait_time).until(
                EC.presence_of_element_located((strategy, locator))
            )
            if element.is_displayed():
                logger.info(f"Element located successfully using {strategy}: {locator}")
                return element
        except TimeoutException:
            logger.warning(f"Timeout while trying {strategy}: {locator}")
        except Exception as e:
            logger.warning(f"Exception while trying {strategy}: {locator} - {e}")
    logger.error("Failed to locate element with all provided locators.")
    return None


# Add the extract_recent_uk_reviews function here
def extract_recent_uk_reviews(review_text):
    uk_review_count = 0
    today = datetime.now()
    three_months_ago = today - timedelta(days=90)

    uk_review_pattern = re.compile(r"Reviewed in the United Kingdom on (\d{1,2} [A-Za-z]+ \d{4})")
    has_uk_reviews = False

    for match in uk_review_pattern.finditer(review_text):
        has_uk_reviews = True
        review_date_str = match.group(1)
        review_date = datetime.strptime(review_date_str, "%d %B %Y")

        if review_date >= three_months_ago:
            uk_review_count += 1

    if not has_uk_reviews:
        return "No UK"

    return uk_review_count

def validate_scraped_data(data):
    """
    Validate scraped data and ensure all required keys are present with default values if missing.
    :param data: Dictionary of scraped data.
    :return: Validated dictionary with defaults for missing values.
    """
    required_keys = ["scan_date", "main_title", "monthly_sold", "rating", "product_info", "variant_reviews", "reviews_text"]
    validated_data = {key: data.get(key, "N/A") for key in required_keys}
    return validated_data

def random_scroll(driver):
    """
    Perform random scrolling to simulate user activity.
    :param driver: Selenium WebDriver instance.
    """
    scroll_height = driver.execute_script("return document.body.scrollHeight")
    for _ in range(random.randint(2, 5)):
        random_scroll_position = random.randint(100, scroll_height // 2)
        driver.execute_script(f"window.scrollTo(0, {random_scroll_position});")
        time.sleep(random.uniform(0.5, 1.5))  # Random pause between scrolls

def scrape_main_page(driver):
    """
    Scrape data from the main product page on Amazon using Selenium.
    :param driver: Selenium WebDriver instance already open on the Amazon product page.
    :return: Validated dictionary of scraped data.
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("Starting main page scraping.")

    try:
        logger.info("Checking if the browser session is valid.")
        # Check if the driver session is valid before proceeding
        if not driver.service.is_connectable():
            logger.error("Browser session is invalid. Skipping scraping.")
            return validate_scraped_data({})

        # Extract the date of the scan
        scan_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Scan date: {scan_date}")

        # Extract main title of the page
        try:
            title_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "productTitle"))
            )
            title_text = title_element.text.strip()
            logger.info(f"Main title extracted: {title_text}")
        except Exception as e:
            title_text = "N/A"
            logger.error(f"Error scraping main title: {e}")

        # Extract the amount sold in a month
        try:
            sold_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//*[@id='socialProofingAsinFaceout_feature_div']"))
            )
            sold_text = sold_element.text.strip()
            logger.info(f"Monthly sold extracted: {sold_text}")
        except Exception as e:
            sold_text = "0"
            logger.error(f"Error scraping monthly sold data: {e}")

        # Extract the product rating
        try:
            rating_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="acrPopover"]/span[1]/a/span'))
            )
            rating_text = rating_element.text.strip()
            logger.info(f"Product rating extracted: {rating_text}")
        except Exception as e:
            rating_text = "N/A"
            logger.error(f"Error scraping product rating: {e}")

        # Extract only the date from the product information
        try:
            product_info_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="productDetailsWithModules_feature_div"]'))
            )
            product_info_text = product_info_element.text.strip()

            # Extract the date from the product information text
            def extract_date(product_info_text):
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

            # Call extract_date and assign the result to extracted_date
            extracted_date = extract_date(product_info_text)

            # Log the extracted date or a warning if no date is found
            if extracted_date:
                logger.info(f"Date extracted from product information: {extracted_date}")
            else:
                extracted_date = "N/A"
                logger.warning("No date found in product information.")

        except Exception as e:
            extracted_date = "N/A"
            logger.error(f"Error scraping product information: {e}")



        # Perform random scrolling and check for reviews link
        try:
            random_scroll(driver)  # Perform random scrolling

            # Attempt to locate the reviews link dynamically
            reviews_link = find_dynamic_element(driver, [
                (By.XPATH, '//*[@id="reviews-medley-footer"]/div[2]/a'),
                (By.XPATH, '//*[@id="reviews-medley-footer"]/div/a'),
                (By.CSS_SELECTOR, '#reviews-medley-footer a')
            ], wait_time=10)

            if not reviews_link:
                logger.error("Unable to locate the reviews link using all methods. Skipping Script 4.")
                return validate_scraped_data({})

            reviews_url = reviews_link.get_attribute('href')  # Extract the URL
            logger.info(f"Extracted reviews URL: {reviews_url}")

            if not reviews_url:
                logger.error("Reviews URL is missing. Skipping Script 4.")
                return validate_scraped_data({})  # Return empty or partial data

            if reviews_url:
                driver.get(reviews_url)
                logger.info("Navigated directly to the reviews page using the extracted URL.")
            else:
                logger.error("Reviews URL is missing. Skipping reviews navigation.")
                return None


            # Wait for the reviews page to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="cm_cr-review_list"]'))
            )
            logger.info("Reviews page fully loaded.")

            # Call Script 4's function with the existing driver and reviews URL
            logger.info(f"Calling test_variant_dropdown with URL: {reviews_url}")
            test_variant_dropdown(driver, reviews_url)
            logger.info("Completed Script 4: Dropdown Interaction.")

            # Extract variant review data
            variant_reviews = "N/A"  # Default value for variant reviews
            try:
                variant_reviews_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="filter-info-section"]'))  # Adjust XPath as needed
                )
                raw_variant_reviews = variant_reviews_element.text.strip()  # Extract and clean up text
                logger.info(f"Raw variant reviews text: {raw_variant_reviews}")

                # Extract only the total ratings using a regular expression
                match = re.search(r'(\d[\d,]*)\s+total ratings', raw_variant_reviews)
                if match:
                    variant_reviews = match.group(1).replace(',', '')  # Remove commas for a clean integer
                    logger.info(f"Variant reviews extracted: {variant_reviews}")
                else:
                    logger.warning(f"No total ratings found in: {raw_variant_reviews}")

            except TimeoutException:
                logger.error("Timeout waiting for variant reviews.")
            except Exception as e:
                logger.error(f"Error extracting variant reviews: {e}")

            # Extract reviews text and calculate UK review count
            reviews_text = "N/A"  # Default value for reviews_text
            try:
                # Locate the reviews section
                reviews_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="cm_cr-review_list"]'))
                )
                raw_reviews_text = reviews_element.text  # Extract raw reviews text

                # Calculate UK reviews in the last 3 months
                uk_review_count = extract_recent_uk_reviews(raw_reviews_text)

                # Update reviews_text based on the review count
                if uk_review_count == "No UK":
                    reviews_text = "No UK"  # No UK reviews at all
                    logger.info("No reviews from the United Kingdom.")
                else:
                    reviews_text = str(uk_review_count)  # Store the count as a string
                    logger.info(f"Number of UK reviews in the last 3 months: {reviews_text}")
            except TimeoutException:
                logger.error("Timeout while waiting for reviews text.")
            except Exception as e:
                logger.error(f"Error extracting and processing reviews text: {e}")



        except Exception as e:
            logger.error(f"Error finding or clicking reviews link: {e}")
            variant_reviews = "N/A"
            reviews_text = "N/A"

        # Return validated data
        scraped_data = {
            "scan_date": scan_date,
            "main_title": title_text,
            "monthly_sold": sold_text,
            "rating": rating_text,
            "product_info": extracted_date,
            "variant_reviews": variant_reviews,
            "reviews_text": reviews_text
        }
        logger.info("Finished main page scraping.")
        return validate_scraped_data(scraped_data)

    except Exception as e:
        logger.error(f"Error scraping main page: {e}")
        return validate_scraped_data({})
    finally:
        if not driver.service.is_connectable():
            logger.info("Browser session already closed or invalid after scraping.")
        logger.info("Main page scraper execution completed.")

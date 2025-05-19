import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from difflib import SequenceMatcher
from bs4 import BeautifulSoup
import time
import random 
import logging

logger = logging.getLogger("Webscrape Bybot")

def handle_overlays(driver):
    """
    Detect and handle overlays like newUserDiv that might block interactions.
    """
    try:
        overlay = driver.find_element(By.ID, "newUserDiv")
        driver.execute_script("arguments[0].style.display = 'none';", overlay)
        logger.info("Overlay hidden successfully.")
    except Exception:
        logger.info("No overlay found or already hidden.")


def clean_seller_name(raw_name):
    """
    Cleans the raw seller name by stripping HTML tags and unnecessary prefixes.
    :param raw_name: Raw seller name string with HTML tags.
    :return: Cleaned seller name.
    """
    try:
        soup = BeautifulSoup(raw_name, "html.parser")
        text = soup.get_text(strip=True)

        # Remove known prefixes (e.g., "FBA Prime", "MF Prime")
        prefixes = ["FBA Prime", "MF Prime"]
        for prefix in prefixes:
            if text.startswith(prefix):
                text = text[len(prefix):].strip()

        return text
    except Exception as e:
        logger.info(f"Error cleaning seller name: {e}")
        return raw_name  # Return the raw name if cleaning fails

def is_similar(brand_name, seller_name, threshold=0.8):
    """
    Checks if the seller name is similar to the brand name.
    :param brand_name: The product's brand name.
    :param seller_name: The seller's name.
    :param threshold: Similarity threshold (default 0.8).
    :return: True if similar, False otherwise.
    """
    cleaned_seller_name = clean_seller_name(seller_name)
    ratio = SequenceMatcher(None, brand_name.lower(), cleaned_seller_name.lower()).ratio()
    logger.info(f"Similarity between '{brand_name}' and '{cleaned_seller_name}': {ratio:.2f}")
    return ratio >= threshold

def validate_scraped_data(data):
    """
    Ensures scraped data contains all required keys with default values if missing.
    :param data: Dictionary of scraped data.
    :return: Validated dictionary with all keys.
    """
    required_keys = ["scan_date", "main_title", "monthly_sold", "rating", "product_info", "variant_reviews", "reviews_text"]
    validated_data = {key: data.get(key, "N/A") for key in required_keys}
    
    # Provide default values for keys if they exist but are empty
    for key in ["monthly_sold", "rating", "product_info", "variant_reviews", "reviews_text"]:
        if not validated_data[key]:
            validated_data[key] = "N/A"
    
    return validated_data



def call_webscraperS2(driver):
    """
    Calls the WebscraperS2 function to scrape additional main page data.
    :param driver: Selenium WebDriver instance.
    :return: Dictionary of scraped data or fallback data if an error occurs.
    """
    driver.switch_to.default_content()
    logger.info("Switched back to the main page.")

    try:
        from WebscraperS2 import scrape_main_page  # Import the WebscraperS2 function
        if driver.session_id:  # Ensure the browser session is still active
            logger.info("Calling WebscraperS2 to scrape additional main page data...")
            scraped_data = scrape_main_page(driver)
            if scraped_data:
                logger.info(f"Scraped Data from S2: {scraped_data}")
                return validate_scraped_data(scraped_data)
            else:
                logger.info("WebscraperS2 returned no data. Using fallback values.")
                return validate_scraped_data({})
        else:
            logger.info("Driver session is invalid. Skipping WebscraperS2.")
            return validate_scraped_data({})
    except Exception as e:
        logger.info(f"Error calling WebscraperS2: {e}")
        return validate_scraped_data({})

def login_to_buybotpro(driver, email, password):
    """
    Logs into BuyBotPro if the login page is detected.
    :param driver: Selenium WebDriver instance.
    :param email: Email address for BuyBotPro login.
    :param password: Password for BuyBotPro login.
    """
    try:
        # Wait for email field
        email_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#loginEmail"))
        )
        logger.info("Email field found.")

        # Wait for password field
        password_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#loginPassword"))
        )
        logger.info("Password field found.")

        # Wait for login button
        login_button = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#loginBtn"))
        )
        logger.info("Login button found.")

        # Enter credentials
        email_field.clear()
        email_field.send_keys(email)
        logger.info("Email entered.")

        password_field.clear()
        password_field.send_keys(password)
        logger.info("Password entered.")

        # Click login button
        login_button.click()
        logger.info("Login button clicked.")

        # Allow time for login process
        time.sleep(5)
    except Exception as e:
        logger.info(f"Error during BuyBotPro login: {e}")

def process_passed_product(asin, break_even_price, min_sell_price, product_cost, row_index, brand_name):
    """
    Handles scraping and testing for passed products on Amazon.
    :param asin: The ASIN or product identifier.
    :param break_even_price: The calculated break-even price.
    :param min_sell_price: The calculated minimum sell price.
    :param product_cost: The cost of the product.
    :param row_index: The row index of the product in Google Sheets.
    :return: Dictionary with result or error information.
    """
    # Selenium setup with undetected_chromedriver
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--allow-running-insecure-content")
    options.add_argument("--disable-infobars")
    options.add_argument("--remote-debugging-port=9222")

    # Set a custom user agent
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.6668.101 Safari/537.36"
    options.add_argument(f"user-agent={user_agent}")

    # Specify the profile path and Chrome executable
    chrome_profile_path = r"C:\\Users\\Luke\\AppData\\Local\\Google\\Chrome\\User Data"
    options.add_argument(f"user-data-dir={chrome_profile_path}")
    options.add_argument("--profile-directory=Profile 1")

    # Initialize Chrome Driver
    try:
        driver = uc.Chrome(options=options)
        logger.info("Chrome Driver started successfully.")
    except Exception as e:
        logger.info("Error starting Chrome Driver:", e)
        return {
            "error": "Failed to start Chrome Driver",
            "asin": asin,
            "break_even_price": break_even_price,
            "min_sell_price": min_sell_price,
            "product_cost": product_cost
        }

    try:
        # Navigate to the Amazon product page
        amazon_url = f"https://www.amazon.co.uk/dp/{asin}"
        logger.info(f"Navigating to {amazon_url} for row {row_index}.")
        driver.get(amazon_url)

        # Example: Scrape the product title
        time.sleep(3)  # Allow the page to load

        # Random scrolling
        for _ in range(3):
            scroll_distance = random.randint(100, 500)
            driver.execute_script(f"window.scrollBy(0, {scroll_distance});")
            time.sleep(1)

        # Locate the BuyBotPro iframe
        try:
            iframe = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "bbp-frame"))
            )
            driver.switch_to.frame(iframe)
            logger.info("Switched to BuyBotPro iframe.")

            # Login to BuyBotPro if necessary
            login_to_buybotpro(driver, "dan@drjhardware.co.uk", "Systembox-60811963")

            time.sleep(2)  # Allow table to refresh

            # Interact with the BuyBotPro input field
            try:
                buy_price_field = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#txtBuyPrice"))
                )
                buy_price_field.clear()
                formatted_cost = f"{product_cost:.2f}"
                buy_price_field.send_keys(formatted_cost)
                logger.info(f"Entered product cost {formatted_cost} into BuyBotPro.")

                # Add a delay to allow ROI to refresh
                time.sleep(5)

                # Check the ROI percentage
                roi_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#quickInfoRoi"))
                )
                roi_text = roi_element.text
                logger.info(f"ROI Percentage: {roi_text}")

                # Extract numerical ROI value and check if it is below 20%
                roi_value = float(roi_text.strip('%'))  # Remove the '%' and convert to float
                if roi_value < 20:
                    logger.info(f"ROI is below 20% ({roi_value}%). Failing and closing the browser.")
                    driver.quit()
                    return {
                        "success": False,
                        "error": "ROI below threshold",
                        "asin": asin,
                        "roi": roi_value
                    }

                # Extract price history from the statistics table
                price_history_table = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#asinAverageStatisticsDataTable"))
                )
                rows = price_history_table.find_elements(By.TAG_NAME, "tr")

                # Extract price data into a dictionary
                history_data = {}
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 5:
                        key = cells[0].text.strip()
                        values = []
                        for cell in cells[1:]:
                            text = cell.text.strip()
                            if text == '-' or not text:
                                values.append(0.0)  # Default value for non-numerical cells
                            else:
                                values.append(float(text))
                        history_data[key] = values

                logger.info("Price History Data:")
                for key, values in history_data.items():
                    logger.info(f"{key}: {values}")

                # Apply price history rules
                amazon_prices = history_data.get("Amazon (£)", [])
                fba_prices = history_data.get("FBA (£)", [])
                buy_box_prices = history_data.get("BuyBox (£)", [])

                def filter_nonzero(prices):
                    return [price for price in prices if price > 0.0]

                amazon_prices = filter_nonzero(amazon_prices)
                fba_prices = filter_nonzero(fba_prices)
                buy_box_prices = filter_nonzero(buy_box_prices)

                if not amazon_prices and not fba_prices and not buy_box_prices:
                    logger.info("All price values are zero or non-existent. Voiding result.")
                    driver.quit()
                    return {"success": False, "error": "All prices are zero or non-existent", "asin": asin}

                # Rule 1: Amazon or FBA prices below break-even
                for period, amazon_price in enumerate(amazon_prices):
                    if amazon_price < break_even_price:
                        logger.info(f"Amazon price ({amazon_price}) below break-even price ({break_even_price}) for period {period}. Failing.")
                        driver.quit()
                        return {"success": False, "error": "Amazon price below break-even", "asin": asin}

                for period, fba_price in enumerate(fba_prices):
                    if fba_price < break_even_price:
                        logger.info(f"FBA price ({fba_price}) below break-even price ({break_even_price}) for period {period}. Failing.")
                        driver.quit()
                        return {"success": False, "error": "FBA price below break-even", "asin": asin}

                # Rule 2: 7-day Amazon or FBA price below min sell price
                if amazon_prices and amazon_prices[0] < min_sell_price:
                    logger.info(f"7-day Amazon price ({amazon_prices[0]}) below min sell price ({min_sell_price}). Failing.")
                    driver.quit()
                    return {"success": False, "error": "7-day Amazon price below min sell price", "asin": asin}

                if fba_prices and fba_prices[0] < min_sell_price:
                    logger.info(f"7-day FBA price ({fba_prices[0]}) below min sell price ({min_sell_price}). Failing.")
                    driver.quit()
                    return {"success": False, "error": "7-day FBA price below min sell price", "asin": asin}

                # Rule 3: BuyBox price 20% or more below FBA prices on average
                if buy_box_prices and fba_prices:
                    buy_box_avg = sum(buy_box_prices) / len(buy_box_prices)
                    fba_avg = sum(fba_prices) / len(fba_prices)
                    if buy_box_avg < fba_avg * 0.8:
                        logger.info(f"BuyBox average price ({buy_box_avg}) is 20% or more below FBA average price ({fba_avg}). Failing.")
                        driver.quit()
                        return {"success": False, "error": "BuyBox price too low compared to FBA", "asin": asin}

                logger.info("Price history passed all rules.")

                try:
                    element = driver.find_element(By.ID, "primeOnly")
                    logger.info("Found #primeOnly element on the page.")
                except Exception:
                    logger.info("No #primeOnly element found on the page.")

                # Filter to show only FBA sellers
                try:
                    fba_filter = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "#primeOnly"))
                    )
                    if not fba_filter.is_selected():
                        fba_filter.click()
                        logger.info("Selected FBA-only sellers.")
                        time.sleep(2)  # Allow table to refresh

                    # Locate and extract the top 3 FBA sellers dynamically
                    sellers = []
                    for i in range(1, 4):
                        try:
                            seller_selector = f"#competitionAnalysisDataTable > tbody > tr:nth-child({i}) > td:nth-child(1) > a"
                            seller_box = WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, seller_selector))
                            )
                            
                            # Handle overlays if present
                            try:
                                overlay = driver.find_element(By.ID, "newUserDiv")
                                driver.execute_script("arguments[0].style.display = 'none';", overlay)
                                logger.info("Overlay hidden successfully.")
                            except Exception:
                                logger.info("No overlay found.")
                            
                            # Scroll the element into view using JavaScript
                            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", seller_box)
                            time.sleep(1)

                            # Extract the seller's name
                            seller_name = seller_box.get_attribute("data-original-title").strip()
                            sellers.append(seller_name)
                            logger.info(f"Seller {i}: {seller_name}")
                        except Exception as e:
                            logger.error(f"Error retrieving seller {i}: {e}")
                            continue  # Skip to the next seller attempt


                    if not sellers:
                        logger.info("No sellers found. Proceeding directly to WebscraperS2.")
                        scraped_data = call_webscraperS2(driver)
                        logger.info(f"Final Scraped Data from WebscraperS2: {scraped_data}")
                        return {"success": True, "scraped_data": scraped_data}

                    # Continue with similarity checks if sellers are found
                    for seller_name in sellers:
                        if is_similar(brand_name, seller_name):  # Use the dynamically passed brand_name
                            logger.info(f"Seller '{seller_name}' is too similar to the brand '{brand_name}'. Failing.")
                            driver.quit()
                            return {"error": "Seller too similar to brand", "seller_name": seller_name}


                    else:
                        # Output all sellers found
                        logger.info(f"Top 3 sellers: {sellers}")

                        # Check similarity of seller names to the brand
                        # Use dynamically passed brand name from the main script
                        for seller_name in sellers:
                            if is_similar(brand_name, seller_name):
                                logger.info(f"Seller '{seller_name}' is too similar to the brand '{brand_name}'. Failing.")
                                driver.quit()
                                return {"error": "Seller too similar to brand", "seller_name": seller_name}

                        # If brand test passes, call WebscraperS2
                        logger.info("Brand test passed. Proceeding to call WebscraperS2.")
                        scraped_data = call_webscraperS2(driver)
                        logger.info(f"Final Scraped Data from WebscraperS2: {scraped_data}")  # Log scraped data
                        return {"success": True, "scraped_data": scraped_data}


                except Exception as e:
                    logger.info(f"Error filtering or retrieving FBA sellers: {e}")

            except Exception as e:
                logger.info(f"Error interacting with BuyBotPro ROI or price history: {e}")
            finally:
                # Switch back to the main content
                driver.switch_to.default_content()
                logger.info("Switched back to the main page.")
        except Exception as e:
            logger.info(f"Error locating BuyBotPro iframe: {e}")

    finally:
        if driver.session_id:  # Check if the browser session is still valid
            logger.info("Closing the browser.")
            driver.quit()
        else:
            logger.info("Browser session already closed.")


if __name__ == "__main__":
    # Example usage for testing
    asin = "B00SWSU5BG"  # Replace with a valid ASIN
    break_even_price = 8.68  # Example break-even price
    min_sell_price = 10.85  # Example minimum sell price
    product_cost = 4.36  # Example product cost
    row_index = 32  # Example row index
    brand_name = "Curaprox"  # Example brand name for testing
    result = process_passed_product(asin, break_even_price, min_sell_price, product_cost, row_index, brand_name)
    logger.info(result)

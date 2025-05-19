import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
import logging
import time
import logging

logger = logging.getLogger("Dropdown Selector")


def test_variant_dropdown(driver, url):
    """
    Handle dropdowns dynamically on the reviews page, trying XPaths and Selectors in order until successful.
    :param driver: Selenium WebDriver instance.
    :param url: URL of the reviews page to interact with.
    """
    logger = logging.getLogger("VariantDropdownTest")
    logger.info(f"test_variant_dropdown called with URL: {url}")

    if not url:
        logger.error("URL is None or missing! Exiting function.")
        return
    driver.get(url)

    # Step 1: Wait for the reviews page to load
    try:
        logger.info("Waiting for the reviews page to load...")
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "cm_cr-review_list"))
        )
        logger.info("Reviews page successfully loaded.")
    except TimeoutException:
        logger.error("Timeout while waiting for the reviews page to load.")
        return

    # Step 2: Interact with 'All variants'
    try:
        logger.info("Attempting to locate the 'All variants' button...")
        all_variants_button_xpath = "//span[text()='All variants']/ancestor::span[contains(@class, 'a-button-text')]"
        all_variants_button = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, all_variants_button_xpath))
        )

        # Scroll into view and click
        driver.execute_script("arguments[0].scrollIntoView(true);", all_variants_button)
        all_variants_button.click()
        logger.info("'All variants' button clicked.")

        # Select an option dynamically
        variant_paths = [
            '//*[@id="a-popover-2"]/div/div/ul/li[2]',  # XPath
            '//*[@id="format-type-dropdown_1"]',        # XPath
            '#a-popover-2 > div > div > ul > li:nth-child(2)',  # CSS Selector
            '#format-type-dropdown_1',                 # CSS Selector
        ]
        if not interact_with_dropdown(driver, variant_paths, logger, "All variants"):
            logger.error("Failed to select an option from the 'All variants' dropdown.")
    except TimeoutException:
        logger.warning("'All variants' button not found. Skipping to 'Top reviews'.")
    except NoSuchElementException:
        logger.warning("'All variants' button not available. Skipping to 'Top reviews'.")
    except Exception as e:
        logger.error(f"Unexpected error interacting with 'All variants': {e}")

    # Step 3: Interact with 'Top reviews'
    try:
        logger.info("Attempting to locate the 'Top reviews' button...")
        top_reviews_button_xpath = "//span[text()='Top reviews']/ancestor::span[contains(@class, 'a-button-text')]"
        top_reviews_button = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, top_reviews_button_xpath))
        )

        # Scroll into view and click
        driver.execute_script("arguments[0].scrollIntoView(true);", top_reviews_button)
        top_reviews_button.click()
        logger.info("'Top reviews' button clicked.")

        # Select an option dynamically
        top_reviews_paths = [
            '//*[@id="a-popover-3"]/div/div/ul/li[2]',  # XPath
            '//*[@id="sort-order-dropdown_1"]',        # XPath
            '#a-popover-3 > div > div > ul > li:nth-child(2)',  # CSS Selector
            '#sort-order-dropdown_1',                 # CSS Selector
        ]
        if not interact_with_dropdown(driver, top_reviews_paths, logger, "Top reviews"):
            logger.error("Failed to select an option from the 'Top reviews' dropdown.")
    except TimeoutException:
        logger.error("Timeout while locating or clicking the 'Top reviews' button.")
    except Exception as e:
        logger.error(f"Unexpected error interacting with 'Top reviews': {e}")


def interact_with_dropdown(driver, paths, logger, dropdown_name):
    """
    Try multiple XPaths and CSS selectors to interact with dropdown options.
    Stops immediately after the first successful interaction.
    :param driver: Selenium WebDriver instance.
    :param paths: List of XPaths and CSS selectors to try.
    :param logger: Logger instance.
    :param dropdown_name: Name of the dropdown (e.g., 'All variants', 'Top reviews').
    :return: True if interaction succeeds, False otherwise.
    """
    for path in paths:
        try:
            if path.startswith('//'):  # XPath
                element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, path))
                )
            else:  # CSS Selector
                element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, path))
                )

            if element.is_displayed():
                logger.info(f"Found {dropdown_name} option: {element.text} using {path}")
                element.click()
                logger.info(f"Clicked {dropdown_name} option: {element.text}")
                return True  # Stop after successful interaction
        except TimeoutException:
            logger.warning(f"Timeout while trying {path} for {dropdown_name}.")
        except StaleElementReferenceException:
            logger.warning(f"Stale element reference encountered for {path}.")
        except Exception as e:
            logger.warning(f"Failed to interact with {path}: {e}")
    return False

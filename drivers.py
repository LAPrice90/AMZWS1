# drivers.py

import os
import time
import logging
import shutil
import filecmp
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

logger = logging.getLogger(__name__)


def patch_chrome_drivers():
    user_dir = os.environ['USERPROFILE']
    uc_driver = os.path.join(user_dir, 'AppData', 'Roaming', 'undetected_chromedriver', 'undetected_chromedriver.exe')
    legacy_driver = os.path.join(user_dir, 'AppData', 'Local', 'Google', 'Chrome', 'chromedriver.exe')
    patch_target = os.path.join(user_dir, 'AppData', 'Local', 'Google', 'Chrome', 'Application', 'chromedriver.exe')

    try:
        if os.path.exists(uc_driver) and os.path.exists(patch_target):
            if not filecmp.cmp(uc_driver, patch_target, shallow=False):
                shutil.copy2(uc_driver, patch_target)
                logger.info("Patched chromedriver.exe with undetected_chromedriver version")
        elif os.path.exists(legacy_driver) and os.path.exists(patch_target):
            if not filecmp.cmp(legacy_driver, patch_target, shallow=False):
                shutil.copy2(legacy_driver, patch_target)
                logger.info("Repatched chromedriver.exe with legacy version")
    except Exception as e:
        logger.warning(f"Driver patching failed: {e}")


def launch_bbp_driver(delay=5):
    logger.info(f"Launching BBP Chrome in {delay} seconds... switch to Webscraper window.")
    time.sleep(delay)

    options = uc.ChromeOptions()
    options.add_argument("--no-first-run --no-service-autorun --password-store=basic")

    driver = uc.Chrome(options=options, use_subprocess=True)
    logger.info("BBP Chrome launched successfully.")
    return driver


def launch_legacy_driver(delay=5):
    logger.info(f"Launching Legacy Chrome in {delay} seconds...")
    time.sleep(delay)

    chrome_path = os.path.join(os.environ['USERPROFILE'], 'AppData', 'Local', 'Google', 'Chrome', 'Application', 'chromedriver.exe')

    chrome_service = Service(chrome_path)
    options = Options()
    options.add_argument("--no-first-run --no-service-autorun --password-store=basic")

    driver = webdriver.Chrome(service=chrome_service, options=options)
    logger.info("Legacy Chrome launched successfully.")
    return driver


def close_browser(driver):
    try:
        logger.info("Closing Chrome browser.")
        driver.quit()
    except Exception as e:
        logger.error(f"Error closing Chrome: {e}")


# sheet_utils.py

import time
import logging

logger = logging.getLogger(__name__)


def retry_google_sheets_operation(operation, *args, **kwargs):
    attempts = 0
    while attempts < 5:
        try:
            return operation(*args, **kwargs)
        except Exception as e:
            logger.warning(f"Google Sheets API error: {e}. Retrying...")
            attempts += 1
            time.sleep(2)
    raise Exception("Google Sheets operation failed after 5 attempts")


def update_cell(sheet, row, col, value):
    retry_google_sheets_operation(sheet.update_cell, row, col, value)


def update_range(sheet, cell_range, values):
    retry_google_sheets_operation(sheet.update, cell_range, values)


def check_a1_status(sheet):
    return sheet.acell("A1").value.strip().upper()

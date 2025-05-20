import logging
import gspread
from auth_helpers import get_credentials
from drivers import patch_chrome_drivers, launch_bbp_driver, close_browser
from sheet_utils import check_a1_status
from product_logic import handle_row


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_scan():
    creds = get_credentials()
    gc = gspread.authorize(creds)
    sheet = gc.open_by_url("<YOUR_GOOGLE_SHEET_URL_HERE>").sheet1

    patch_chrome_drivers()
    driver = launch_bbp_driver()

    try:
        total_rows = sheet.row_count
        row_index = 2  # Start from second row

        while row_index <= total_rows:
            a1_status = check_a1_status(sheet)
            if a1_status == "STOP":
                logger.info("A1 status is STOP — exiting scan early.")
                break

            handle_row(sheet, row_index)
            row_index += 1

    finally:
        close_browser(driver)


if __name__ == "__main__":
    run_scan()

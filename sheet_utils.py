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
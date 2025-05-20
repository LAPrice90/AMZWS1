import logging
from sheet_utils import update_cell
from tokenCall import get_access_token
from amazonCatalogCall import get_catalog_details
from hazmatCall import check_eligibility_for_asin
from pricingCall import get_pricing_details_for_asin
from feeCall import get_fees_estimate_for_asin
from Webscrape import process_passed_product

logger = logging.getLogger(__name__)


def handle_row(sheet, row_index):
    try:
        token = get_access_token()
        asin = sheet.cell(row_index, 3).value
        url = sheet.cell(row_index, 4).value

        if not asin:
            update_cell(sheet, row_index, 53, "NOASIN")
            logger.warning(f"Row {row_index}: No ASIN found — marked NOASIN")
            return

        cat_result = get_catalog_details(token, asin)
        if cat_result == "NO DATE":
            update_cell(sheet, row_index, 53, "NODATE")
            logger.warning(f"Row {row_index}: No DATE found — marked NODATE")
            return

        hazmat_result = check_eligibility_for_asin(token, asin)
        if hazmat_result == "hazardous":
            update_cell(sheet, row_index, 53, "HAZMATFAIL")
            logger.warning(f"Row {row_index}: HAZMAT flagged — marked HAZMATFAIL")
            return

        pricing_result = get_pricing_details_for_asin(token, asin)
        if pricing_result == "NO COMPETITOR DATA":
            update_cell(sheet, row_index, 53, "NOCOMPARE")
            logger.warning(f"Row {row_index}: No competitor data — marked NOCOMPARE")
            return

        fees_result = get_fees_estimate_for_asin(token, asin)
        if fees_result == "NOCOST":
            update_cell(sheet, row_index, 53, "NOCOST")
            logger.warning(f"Row {row_index}: No cost data — marked NOCOST")
            return

        err_reason = process_passed_product(sheet, row_index, cat_result, hazmat_result, pricing_result, fees_result)

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

    except Exception as e:
        update_cell(sheet, row_index, 53, "ERROR")
        logger.error(f"Row {row_index}: Unhandled error — marked ERROR: {e}")

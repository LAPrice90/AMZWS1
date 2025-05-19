import requests
import time
from tokenCall import get_access_token
from amazonCatalogCall import get_catalog_details
from hazmatCall import check_eligibility_for_asin
from pricingCall import get_pricing_details_for_asin
from feeCall import get_fees_estimate_for_asin
from Webscrape import process_passed_product
import gspread
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
import pickle
import os
import logging

# Centralized Logging Configuration
LOG_FILE = "script1_log.txt"
logging.basicConfig(
    level=logging.INFO,  # Set log level (INFO for general logs, DEBUG for detailed logs)
    format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),  # Save logs to file
        logging.StreamHandler()  # Print logs to terminal
    ]
)

logger = logging.getLogger("Script1")
logger.info("Logging setup complete.")


def retry_google_sheets_operation(func, *args, **kwargs):
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            if "Quota exceeded" in str(e):
                logger.info(f"Quota exceeded. Retrying in {retry_delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
            else:
                raise
        except Exception as e:
            logger.info(f"Unexpected error during Google Sheets operation: {e}")
            raise
    logger.info("Max retries reached. Operation failed.")
    return None

max_retries = 5  # Number of retry attempts
retry_delay = 60  # Wait time in seconds before retrying

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
CLIENT_SECRET_FILE = r"C:\\Users\\Luke\\Desktop\\Automation\\client_secret_451363787295-6aivcb277tha61cut64a8l288ui8hnba.apps.googleusercontent.com.json"

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

try:
    client = gspread.authorize(creds)
    sheet = client.open('Amazon API 2.0').worksheet('First Checks')
    logger.info("Successfully connected to Google Sheet.")

    # Determine the starting row (defaulting to 2 if invalid or missing)
    start_row = retry_google_sheets_operation(sheet.cell, 1, 1).value
    if not start_row or not start_row.isdigit():
        logger.info("Invalid start row value in A1. Defaulting to row 2.")
        start_row = 2
    else:
        start_row = int(start_row)

    # Ensure total rows account for all rows with any data
    total_rows = max(len(sheet.col_values(1)), len(sheet.col_values(2)))  # Use the column with maximum data
    if total_rows == 0:
        logger.info("No rows detected in the Google Sheet. Please check your sheet for data.")
        total_rows = 1  # Fallback to 1 for header row

    # Set up row_index to start processing
    row_index = start_row
    logger.info(f"Processing rows from {start_row} to {total_rows + 1}...")

    # Check A1 for termination condition and retry logic
    try:
        retries = 0
        while retries < 5:  # Retry up to 5 times
            start_row_value = retry_google_sheets_operation(sheet.cell, 1, 1).value

            if start_row_value.strip().lower() == "completed":
                logger.info("A1 contains 'Completed'. Terminating the script.")
                exit(0)  # Exit the script

            if start_row_value.isdigit():
                start_row = int(start_row_value)  # Set start_row to the value in A1
                row_index = start_row  # Initialize row_index with start_row
                logger.info(f"Starting row set to {row_index} based on A1 value.")
                break
            else:
                logger.warning(f"A1 contains an invalid value: '{start_row_value}'. Retrying in 1 second...")
                retries += 1
                time.sleep(1)

        if retries == 5:  # If retries are exhausted
            logger.error("A1 contains invalid data after 5 attempts. Terminating the script.")
            exit(1)  # Exit with an error code
    except Exception as e:
        logger.error(f"Unexpected error while checking A1: {e}")
        exit(1)  # Exit with an error code

    # Processing loop
    while row_index <= total_rows:
        try:
            # Refresh token dynamically within the loop
            access_token = get_access_token()

            # Check if column BA already has a value
            column_ba_value = retry_google_sheets_operation(sheet.cell, row_index, 53).value

            if column_ba_value:
                logger.info(f"Skipping row {row_index} as it already has values in column BA.")
                start_row_value = retry_google_sheets_operation(sheet.cell, 1, 1).value

                if start_row_value.strip().lower() == "completed":
                    logger.info("A1 contains 'Completed'. Terminating the script.")
                    exit(0)  # Exit the script

                if start_row_value.isdigit():
                    row_index = int(start_row_value)
                else:
                    logger.warning(f"A1 contains an invalid value: '{start_row_value}'. Using default row_index 2.")
                    row_index = 2

                continue

            # Process the row
            barcode = retry_google_sheets_operation(sheet.cell, row_index, 2).value
            if not barcode:
                logger.info(f"No barcode found in row {row_index}, skipping.")
                start_row_value = retry_google_sheets_operation(sheet.cell, 1, 1).value

                if start_row_value.strip().lower() == "completed":
                    logger.info("A1 contains 'Completed'. Terminating the script.")
                    exit(0)  # Exit the script

                if start_row_value.isdigit():
                    row_index = int(start_row_value)
                else:
                    logger.warning(f"A1 contains an invalid value: '{start_row_value}'. Using default row_index 2.")
                    row_index = 2
                continue

            logger.info(f"Processing row {row_index} with barcode {barcode}.")

            details = get_catalog_details(barcode, access_token)

            # Handle missing or invalid details
            if details is None or "asin" not in details:
                logger.info(f"No valid ASIN found for barcode {barcode} in row {row_index}. Logging FAIL.")
                retry_google_sheets_operation(sheet.update_cell, row_index, 53, "FAIL")
                row_index += 1
                continue

            # Process valid details
            brand_name = details.get("brand", "Unknown")  # Fetch the brand dynamically
            rank = details.get("rank", float("inf"))
            if rank > 50000:
                retry_google_sheets_operation(sheet.update_cell, row_index, 53, "FAIL")
            else:
                dimensions = details.get("dimensions", {})
                height = round(dimensions.get("height", {}).get("value", 0) * 25.4)
                width = round(dimensions.get("width", {}).get("value", 0) * 25.4)
                length = round(dimensions.get("length", {}).get("value", 0) * 25.4)
                weight = details.get("weight", "N/A")

                if isinstance(weight, (int, float)):
                    weight = round(weight * 453.592 if weight > 0 else 0)
                else:
                    weight = 0

                retry_google_sheets_operation(sheet.update, f"F{row_index}:M{row_index}", [[
                    details.get("asin", "N/A"),
                    rank,
                    details.get("release_date", "N/A"),
                    details.get("brand", "N/A"),
                    height,
                    width,
                    length,
                    weight
                ]])

                asin = details.get("asin", "N/A")
                if asin != "N/A":
                    eligibility_result = check_eligibility_for_asin(asin, access_token)

                    # Handle hazmat eligibility
                    if eligibility_result.get("eligible"):
                        retry_google_sheets_operation(sheet.update_cell, row_index, 14, "Yes")
                        logger.info(f"ASIN {asin} passed hazmat eligibility.")

                        pricing_result = get_pricing_details_for_asin(asin, access_token)
                        time.sleep(30)
                        buy_box_price = pricing_result.get("buy_box_price", "N/A")
                        lowest_afn_price = pricing_result.get("lowest_afn_price", "N/A")

                        try:
                            buy_box_price = float(buy_box_price) if buy_box_price != "N/A" else 0
                            lowest_afn_price = float(lowest_afn_price) if lowest_afn_price != "N/A" else 0
                        except ValueError:
                            buy_box_price = 0
                            lowest_afn_price = 0

                        retry_google_sheets_operation(sheet.update, f"P{row_index}:Q{row_index}", [[buy_box_price, lowest_afn_price]])

                        final_price = max(lowest_afn_price, buy_box_price)
                        for fee_attempt in range(3):
                            fee_result = get_fees_estimate_for_asin(asin, final_price, access_token)
                            if "error" not in fee_result:
                                break
                            logger.info(f"Retrying fee estimate for ASIN {asin} (Attempt {fee_attempt + 1}/3)...")
                            time.sleep(1)

                        referral_fee = round(fee_result.get("referral_fee", 0), 2)
                        fba_fee = round(fee_result.get("fba_fee", 0), 2)

                        referral_fee_percent = round((referral_fee / final_price) * 100 if final_price > 0 else 0, 0)
                        digital_fee = round((fba_fee + referral_fee) * 0.02, 2)
                        estimated_shipping = round(weight * 0.0002045 if weight else 0, 2)
                        product_cost = round(float(retry_google_sheets_operation(sheet.cell, row_index, 3).value or 0), 2)

                        vat_rate = float(retry_google_sheets_operation(sheet.cell, row_index, 4).value or 0) / 100
                        vat_adjusted_price = round(buy_box_price / (1 + vat_rate), 2) if buy_box_price else 0
                        vat_expense = round(product_cost * vat_rate, 2) if vat_rate else 0

                        total_costs = (round(product_cost + fba_fee + digital_fee + estimated_shipping, 2)) * 1.03
                        break_even_price = round((total_costs * (1 + vat_rate)) * (1 + (referral_fee_percent / 100)), 2)
                        min_sell_price = round(break_even_price * 1.20, 2)

                        reasonable_price = buy_box_price

                        retry_google_sheets_operation(sheet.update, f"R{row_index}:Z{row_index}", [[
                            "N/A",
                            reasonable_price,
                            fba_fee,
                            referral_fee,
                            digital_fee,
                            estimated_shipping,
                            vat_adjusted_price,
                            break_even_price,
                            min_sell_price
                        ]])
                        if min_sell_price > reasonable_price:
                            retry_google_sheets_operation(sheet.update_cell, row_index, 53, "FAIL")
                        else:
                            # Reset scraped_data at the start of each row processing
                            scraped_data = None

                            scraped_data = process_passed_product(
                                asin=asin,
                                break_even_price=break_even_price,
                                min_sell_price=min_sell_price,
                                product_cost=product_cost,
                                row_index=row_index,
                                brand_name=brand_name
                            ) or {"success": False, "scraped_data": {}}

                            logger.info(f"Brand Name for row {row_index}: {brand_name}")

                            # Log the scraped data to confirm its association with the current row
                            logger.info(f"Scraped Data for row {row_index}: {scraped_data}")

                            # Additional validation
                            if scraped_data.get("success") and "scraped_data" in scraped_data:
                                data = scraped_data["scraped_data"]
                                scan_date = data.get("scan_date", "")
                                main_title = data.get("main_title", "")
                                monthly_sold = data.get("monthly_sold", "")
                                rating = data.get("rating", "")
                                product_info = data.get("product_info", "")
                                variant_reviews = data.get("variant_reviews", "")
                                reviews_text = data.get("reviews_text", "")

                                # Log extracted data with reviews_text and product_info limited to 40 characters
                                logger.info(f"Extracted Data for Google Sheets: scan_date={scan_date}, main_title={main_title}, "
                                    f"monthly_sold={monthly_sold}, rating={rating}, product_info={product_info[:40]}, "
                                    f"variant_reviews={variant_reviews}, reviews_text={reviews_text[:40]}")

                                # Update Google Sheets with validated data
                                try:
                                    retry_google_sheets_operation(
                                        sheet.update,
                                        f"AA{row_index}:AG{row_index}",
                                        [[scan_date, main_title, monthly_sold, rating, product_info, variant_reviews, reviews_text]]
                                    )
                                    logger.info(f"Scraped data successfully updated for row {row_index}.")
                                except Exception as e:
                                    logger.info(f"Error updating Google Sheets for row {row_index}: {e}")
                                    retry_google_sheets_operation(sheet.update_cell, row_index, 53, "FAIL")

                                # Dynamically set the formula for column AH in the current row
                                formula = f"""=iferror(if(VALUE(AF{row_index})>((today()-AE{row_index})/365)*40,if(VALUE(AF{row_index})>((today()-AE{row_index})/365)*50,1,0.5),0),0) + \
                                iferror(if(VALUE(AD{row_index})>=3.5,if(VALUE(AD{row_index})>=4,1,0.5),0),0) + \
                                iferror(if(VALUE(AG{row_index})>=6,1,0),0) + \
                                iferror(if(VALUE(AF{row_index})>=6,1,0),0) + 1"""

                                try:
                                    retry_google_sheets_operation(sheet.update_cell, row_index, 34, formula)  # Column AH is 34
                                    logger.info(f"Formula successfully pasted in row {row_index}, column AH.")
                                except Exception as e:
                                    logger.error(f"Error pasting formula in row {row_index}, column AH: {e}")

                                # Dynamically set the formula for column BA in the current row
                                formula_ba = f"=if(AH{row_index}>=3.5,\"PASS\",\"FAIL\")"

                                try:
                                    retry_google_sheets_operation(sheet.update_cell, row_index, 53, formula_ba)  # Column BA is 53
                                    logger.info(f"Formula successfully pasted in row {row_index}, column BA.")
                                except Exception as e:
                                    logger.error(f"Error pasting formula in row {row_index}, column BA: {e}")

                            else:
                                logger.info(f"Failed to retrieve valid scraped data for row {row_index}. Logging FAIL.")
                                retry_google_sheets_operation(sheet.update_cell, row_index, 53, "FAIL")

                    else:
                        logger.info(f"ASIN {asin} is not hazmat eligible. Logging FAIL for row {row_index}.")
                        retry_google_sheets_operation(sheet.update_cell, row_index, 53, "FAIL")

                        # Ensure further processing is skipped for this row
                        row_index += 1
                        continue

        except IndexError as e:
            logger.info(f"IndexError encountered at row {row_index}: {e}")
        except Exception as e:
            logger.error(f"Unhandled exception for row {row_index}: {e}")
            start_row_value = retry_google_sheets_operation(sheet.cell, 1, 1).value

            if start_row_value.strip().lower() == "completed":
                logger.info("A1 contains 'Completed'. Terminating the script.")
                exit(0)  # Exit the script

            if start_row_value.isdigit():
                row_index = int(start_row_value)
            else:
                logger.warning(f"A1 contains an invalid value: '{start_row_value}'. Using default row_index 2.")
                row_index = 2


        start_row_value = retry_google_sheets_operation(sheet.cell, 1, 1).value

        if start_row_value.strip().lower() == "completed":
            logger.info("A1 contains 'Completed'. Terminating the script.")
            exit(0)  # Exit the script

        if start_row_value.isdigit():
            row_index = int(start_row_value)
        else:
            logger.warning(f"A1 contains an invalid value: '{start_row_value}'. Using default row_index 2.")
            row_index = 2


except Exception as e:
    logger.info("Error accessing Google Sheet or processing data:", e)

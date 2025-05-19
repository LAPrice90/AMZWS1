import requests
import time

def get_fees_estimate_for_asin(asin, final_price, access_token, retries=5):
    url = f"https://sellingpartnerapi-eu.amazon.com/products/fees/v0/items/{asin}/feesEstimate"
    headers = {
        "Accept": "application/json",
        "x-amz-access-token": access_token,
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "FeesEstimateRequest": {
            "MarketplaceId": "A1F83G8C2ARO7P",
            "IsAmazonFulfilled": True,
            "PriceToEstimateFees": {
                "ListingPrice": {"Amount": final_price, "CurrencyCode": "GBP"},
                "Shipping": {"Amount": 0.00, "CurrencyCode": "GBP"}
            },
            "Identifier": "UniqueIdentifier123"
        }
    }

    for attempt in range(retries):
        try:
            response = requests.post(url, headers=headers, json=payload)
            if response.status_code == 200:
                json_response = response.json()

                if json_response.get("payload") and json_response["payload"].get("FeesEstimateResult"):
                    fee_details = json_response["payload"]["FeesEstimateResult"]["FeesEstimate"]["FeeDetailList"]

                    referral_fee_total = sum(fee["FeeAmount"]["Amount"] for fee in fee_details if fee["FeeType"] == "ReferralFee")
                    fba_fee_total = sum(fee["FeeAmount"]["Amount"] for fee in fee_details if fee["FeeType"] == "FBAFees")

                    return {
                        "asin": asin,
                        "referral_fee": referral_fee_total,
                        "fba_fee": fba_fee_total
                    }
                else:
                    print(f"No fee estimate data found for ASIN {asin}.")
                    return {"asin": asin, "error": "No fee data"}
            elif response.status_code == 429:  # Quota limit hit
                print(f"Quota limit hit for ASIN {asin}. Retrying in 1 second... (Attempt {attempt + 1}/{retries})")
                time.sleep(1)  # Wait before retrying
            else:
                print(f"Error retrieving fee estimate for ASIN {asin}: {response.status_code} - {response.text}")
                return {"asin": asin, "error": f"Error {response.status_code}"}
        except Exception as e:
            print(f"Exception during fee estimate retrieval for ASIN {asin}: {e}")
            time.sleep(2)  # Wait before retrying
    return {"asin": asin, "error": "Failed after retries"}

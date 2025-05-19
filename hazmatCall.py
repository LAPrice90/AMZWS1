import requests
import time
from tokenCall import get_access_token

def check_eligibility_for_asin(asin, access_token, retries=5):
    url = f"https://sellingpartnerapi-eu.amazon.com/fba/inbound/v1/eligibility/itemPreview?asin={asin}&program=INBOUND&marketplaceIds=A1F83G8C2ARO7P"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }

    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                json_response = response.json()
                is_eligible = json_response.get("payload", {}).get("isEligibleForProgram", False)
                ineligibility_reasons = json_response.get("payload", {}).get("ineligibilityReasonList", [])

                return {
                    "asin": asin,
                    "eligible": is_eligible,
                    "reasons": ineligibility_reasons
                }
            elif response.status_code == 429:  # Quota limit hit
                print(f"Quota limit hit for ASIN {asin}. Retrying in 1 second... (Attempt {attempt + 1}/{retries})")
                time.sleep(1)  # Wait before retrying
            else:
                return {
                    "asin": asin,
                    "eligible": False,
                    "error": f"Error {response.status_code}"
                }
        except Exception as e:
            print(f"Exception during eligibility check for ASIN {asin}: {e}")
            time.sleep(1)  # Wait before retrying
    return {
        "asin": asin,
        "eligible": False,
        "error": "Failed after retries"
    }

if __name__ == "__main__":
    try:
        # Fetch access token
        access_token = get_access_token()

        # Example ASINs passed from the main script
        asins = []  # Main script should populate this list dynamically

        # Loop through the ASINs and check eligibility
        for asin in asins:
            result = check_eligibility_for_asin(asin, access_token)
            print(result)

            # Pause to respect rate limits
            time.sleep(2)

    except Exception as e:
        print("Error during processing:", e)

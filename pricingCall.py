import requests
import time

def get_pricing_details_for_asin(asin, access_token, retries=5):
    url = "https://sellingpartnerapi-eu.amazon.com/batches/products/pricing/2022-05-01/items/competitiveSummary"
    headers = {
        "Accept": "application/json",
        "x-amz-access-token": access_token,
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "requests": [
            {
                "marketplaceId": "A1F83G8C2ARO7P",
                "asin": asin,
                "includedData": ["featuredBuyingOptions", "lowestPricedOffers"],
                "lowestPricedOffersInputs": [
                    {
                        "itemCondition": "New",
                        "offerType": "Consumer"
                    }
                ],
                "method": "GET",
                "uri": "/products/pricing/2022-05-01/items/competitiveSummary"
            }
        ]
    }

    for attempt in range(retries):
        try:
            response = requests.post(url, headers=headers, json=payload)
            status_code = response.status_code

            if status_code == 200:
                json_response = response.json()
                response_body = json_response["responses"][0].get("body", {})

                if response_body:
                    buy_box_price = response_body.get("featuredBuyingOptions", [])[0].get("segmentedFeaturedOffers", [{}])[0].get("listingPrice", {}).get("amount", "N/A")
                    lowest_afn_price = response_body.get("lowestPricedOffers", [{}])[0].get("offers", [{}])[0].get("listingPrice", {}).get("amount", "N/A")

                    return {
                        "asin": asin,
                        "buy_box_price": buy_box_price,
                        "lowest_afn_price": lowest_afn_price
                    }
                else:
                    print(f"No data found for ASIN {asin}.")
                    return {"asin": asin, "error": "No data"}
            elif status_code == 429:  # Quota limit hit
                print(f"Quota limit hit for ASIN {asin}. Retrying in 5 seconds... (Attempt {attempt + 1}/{retries})")
                time.sleep(5)  # Wait before retrying
            else:
                print(f"Error retrieving pricing data for ASIN {asin}: {status_code} - {response.text}")
                return {"asin": asin, "error": f"Error {status_code}"}
        except Exception as e:
            print(f"Exception during pricing data retrieval for ASIN {asin}: {e}")
            time.sleep(2)  # Wait before retrying
    return {"asin": asin, "error": "Failed after retries"}

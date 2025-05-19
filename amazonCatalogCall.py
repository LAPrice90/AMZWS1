import requests
import time
from tokenCall import get_access_token

def get_catalog_details(barcode, access_token):
    """
    Fetch catalog details for a given barcode using the Amazon Catalog API.

    Args:
        barcode (str): The barcode (UPC) to look up.
        access_token (str): The access token for Amazon's Selling Partner API.

    Returns:
        dict: A dictionary containing ASIN, rank, brand, dimensions, weight, and other metadata.
    """
    # API endpoint and parameters
    url = f"https://sellingpartnerapi-eu.amazon.com/catalog/2022-04-01/items?identifiers={barcode}&identifiersType=UPC&marketplaceIds=A1F83G8C2ARO7P&includedData=attributes,dimensions,salesRanks,classifications,summaries"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }

    try:
        # Send GET request
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            # Parse JSON response
            data = response.json()
            items = data.get("items", [])
            if not items:
                print(f"No items found for barcode {barcode}.")
                return None
            item = items[0]

            # Extract required details
            dimensions = item.get("dimensions", [{}])[0].get("package", {})
            weight_data = dimensions.get("weight", {})

            # Extract weight in pounds if available, fallback to attributes
            weight = weight_data.get("value") if weight_data.get("unit") == "pounds" else None
            if not weight:
                weight = item.get("attributes", {}).get("item_weight", [{}])[0].get("value", "N/A")

            result = {
                "asin": item.get("asin", "N/A"),
                "rank": item.get("salesRanks", [{}])[0].get("displayGroupRanks", [{}])[0].get("rank", "N/A"),
                "brand": item.get("attributes", {}).get("brand", [{}])[0].get("value", "N/A"),
                "dimensions": dimensions,
                "weight": weight,
                "release_date": item.get("summaries", [{}])[0].get("releaseDate", "N/A")
            }
            return result
        elif response.status_code == 429:  # Quota limit hit
            print(f"Quota limit hit for barcode {barcode}. Skipping this barcode.")
            return None
        else:
            print(f"Error: HTTP {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Exception during API request: {e}")
        return None

# This script is designed to be called by the main script with barcodes dynamically provided.

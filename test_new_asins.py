# test_new_asins.py
"""
Description:
  Manually test new ASINs using the Chart_Test run_bbp_chart_analysis() function,
  without connecting to Google Sheets or your main pipeline.

Usage:
  python test_new_asins.py
"""

import os
import pandas as pd
from Chart_Test import run_bbp_chart_analysis

def test_new_asins():
    """
    Manually define a list of new ASINs to test with cost + vat,
    scrape them via the Chart_Test logic, and save each result.
    """

    # A single example for now:
    test_asins = [
        {"asin": "B0933HNJ8H", "cost": 8.43, "vat": 20.0},
    ]

    csv_folder = "csv_data"
    os.makedirs(csv_folder, exist_ok=True)

    for item in test_asins:
        asin = item["asin"]
        cost = item["cost"]
        vat = item["vat"]

        print(f"\n--- Testing {asin} with cost=£{cost} and VAT={vat}% ---")

        # The run_bbp_chart_analysis will create its own Chrome driver if none is passed
        df_result = run_bbp_chart_analysis(asin, cost, vat)

        # If data is empty, no result was found (maybe BBP scraping failed or product not found)
        if df_result.empty:
            print(f"⚠️ No data returned for {asin}.")
            continue

        # Save CSV
        out_path = os.path.join(csv_folder, f"output_{asin}.csv")
        df_result.to_csv(out_path, index=False)
        print(f"✅ Results saved to: {out_path} (rows={len(df_result)})")

        # Optional quick summary
        # e.g. show the final rating from the last row
        if "ROI Score" in df_result.columns:
            final_score = df_result["ROI Score"].iloc[-1]
            print(f"ROI Score (last row): {final_score}")

if __name__ == "__main__":
    test_new_asins()
    print("\nAll done with manual test run!")

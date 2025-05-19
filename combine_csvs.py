import os
import pandas as pd

# Set up folders
input_folder = "csv_data"
output_folder = "combined_exports"
os.makedirs(output_folder, exist_ok=True)

# Merge all CSVs from csv_data folder
df_all = pd.concat(
    [pd.read_csv(os.path.join(input_folder, f)) for f in os.listdir(input_folder) if f.endswith(".csv")],
    ignore_index=True
)

# Add calculated review share and adjusted actual sales if columns are present
if "Scraper2_VariantReviews" in df_all.columns and "Scraper2_ParentTotalReviews" in df_all.columns:
    df_all["Review Share %"] = df_all.apply(
        lambda row: 1.0 if row["Scraper2_ParentTotalReviews"] == 0 else
                    min(1.0, row["Scraper2_VariantReviews"] / row["Scraper2_ParentTotalReviews"]),
        axis=1
    )
    if "Actual Sales Volume" in df_all.columns:
        df_all["Adjusted Actual Sales"] = df_all["Actual Sales Volume"] / df_all["Review Share %"]
    else:
        df_all["Adjusted Actual Sales"] = None
else:
    df_all["Review Share %"] = None
    df_all["Adjusted Actual Sales"] = None

# Add theory-based estimated sales from BSR if BSR and review share exist
if "BSR" in df_all.columns and "Review Share %" in df_all.columns:
    df_all["Estimated Sales (Theory)"] = df_all.apply(
        lambda row: 6140.42 * (row["BSR"] ** -0.879) / row["Review Share %"]
        if pd.notna(row["BSR"]) and row["BSR"] > 0 and pd.notna(row["Review Share %"]) and row["Review Share %"] > 0 else None,
        axis=1
    )
else:
    df_all["Estimated Sales (Theory)"] = None

# Apply multipliers for Source and Offer Count
source_multipliers = {
    "Amazon": 1.00,
    "FBA": 1.00,
    "FBM": 1.15
}

if "Chosen Source" not in df_all.columns and "Source" in df_all.columns:
    df_all["Chosen Source"] = df_all["Source"]
    print("ℹ️ Using 'Source' column as 'Chosen Source'")

df_all["Source Multiplier"] = df_all["Chosen Source"].map(source_multipliers).fillna(1.0)

def get_offer_multiplier(count):
    if count <= 3:
        return 1.0
    elif count <= 6:
        return 0.9
    elif count <= 10:
        return 0.8
    elif count <= 15:
        return 0.7
    elif count <= 20:
        return 0.6
    else:
        return 0.5

df_all["Offer Multiplier"] = df_all["Offer Count"].apply(lambda x: get_offer_multiplier(x) if pd.notna(x) else 1.0)

# Final adjusted sales estimate
if "Estimated Sales (Theory)" in df_all.columns:
    df_all["Adjusted Sales (Full Logic)"] = df_all["Estimated Sales (Theory)"] * df_all["Source Multiplier"] * df_all["Offer Multiplier"]
else:
    df_all["Adjusted Sales (Full Logic)"] = None

# Add BSR Bands
bsr_bins = [0, 100, 500, 1000, 2500, 5000, 10000, 25000, 50000, float('inf')]
bsr_labels = ["0–100", "101–500", "501–1,000", "1,001–2,500", "2,501–5,000",
              "5,001–10,000", "10,001–25,000", "25,001–50,000", "50,000+"]
df_all["BSR Band"] = pd.cut(df_all["BSR"], bins=bsr_bins, labels=bsr_labels, right=True)

# --- New Step: Category-Based Sales Curve ---
if "Category" in df_all.columns and "BSR Band" in df_all.columns and "Adjusted Actual Sales" in df_all.columns:
    avg_sales_by_cat_bsr = df_all.groupby(["Category", "BSR Band"])["Adjusted Actual Sales"].mean().reset_index()
    avg_sales_by_cat_bsr.columns = ["Category", "BSR Band", "Estimated Sales (Category Curve)"]
    df_all = df_all.merge(avg_sales_by_cat_bsr, on=["Category", "BSR Band"], how="left")
else:
    df_all["Estimated Sales (Category Curve)"] = None

# Final sales estimate preference: category curve > full logic > theory
if "Estimated Sales (Category Curve)" in df_all.columns and "Adjusted Sales (Full Logic)" in df_all.columns:
    df_all["Final Estimated Sales"] = df_all["Estimated Sales (Category Curve)"].combine_first(df_all["Adjusted Sales (Full Logic)"])
else:
    df_all["Final Estimated Sales"] = df_all["Estimated Sales (Theory)"]

# --- New: Model Error Calculations ---
if "Adjusted Actual Sales" in df_all.columns:
    df_all["Error - Theory vs Actual"] = (df_all["Adjusted Actual Sales"] - df_all["Estimated Sales (Theory)"]).abs()
    df_all["Error - Full Logic vs Actual"] = (df_all["Adjusted Actual Sales"] - df_all["Adjusted Sales (Full Logic)"]).abs()
    df_all["Error - Category vs Actual"] = (df_all["Adjusted Actual Sales"] - df_all["Estimated Sales (Category Curve)"]).abs()

    df_all["Accuracy - Theory"] = (df_all["Error - Theory vs Actual"] / df_all["Adjusted Actual Sales"]).apply(lambda x: 1 - x if pd.notna(x) else None)
    df_all["Accuracy - Full Logic"] = (df_all["Error - Full Logic vs Actual"] / df_all["Adjusted Actual Sales"]).apply(lambda x: 1 - x if pd.notna(x) else None)
    df_all["Accuracy - Category"] = (df_all["Error - Category vs Actual"] / df_all["Adjusted Actual Sales"]).apply(lambda x: 1 - x if pd.notna(x) else None)

    # New block: Group-level accuracy per ASIN
    if "ASIN" in df_all.columns:
        asin_group = df_all.groupby("ASIN")
        df_accuracy_asin = asin_group[["Adjusted Actual Sales", "Estimated Sales (Theory)", "Adjusted Sales (Full Logic)", "Estimated Sales (Category Curve)"]].sum().reset_index()
        df_accuracy_asin["Accuracy - Theory"] = 1 - (df_accuracy_asin["Estimated Sales (Theory)"] - df_accuracy_asin["Adjusted Actual Sales"]
).abs() / df_accuracy_asin["Adjusted Actual Sales"]
        df_accuracy_asin["Accuracy - Full Logic"] = 1 - (df_accuracy_asin["Adjusted Sales (Full Logic)"] - df_accuracy_asin["Adjusted Actual Sales"]).abs() / df_accuracy_asin["Adjusted Actual Sales"]
        df_accuracy_asin["Accuracy - Category"] = 1 - (df_accuracy_asin["Estimated Sales (Category Curve)"] - df_accuracy_asin["Adjusted Actual Sales"]).abs() / df_accuracy_asin["Adjusted Actual Sales"]
        df_accuracy_asin.to_csv(os.path.join(output_folder, "accuracy_by_asin.csv"), index=False)
        print("✅ ASIN-level accuracy saved to combined_exports/accuracy_by_asin.csv")

# Save full dataset
df_all.to_csv(os.path.join(output_folder, "merged_full_dataset.csv"), index=False)
print("✅ Merged data saved to combined_exports/merged_full_dataset.csv")

# Add ROI 10% flag
if "ROI %" in df_all.columns:
    df_all["Below Min ROI (10%)"] = df_all["ROI %"] < 10
else:
    df_all["Below Min ROI (10%)"] = False

# Add Offer Count Buckets
offer_bins = [0, 3, 6, 10, 15, 20, float('inf')]
offer_labels = ["1–3", "4–6", "7–10", "11–15", "16–20", "21+"]
df_all["Offer Count Bucket"] = pd.cut(df_all["Offer Count"], bins=offer_bins, labels=offer_labels, right=True)

# Save updated dataset with ROI flag and Offer Count Bucket
df_all.to_csv(os.path.join(output_folder, "merged_full_dataset.csv"), index=False)
print("✅ ROI check (10%) and Offer Count Buckets added to merged_full_dataset.csv")

# Summary columns
summary_cols = ["Actual Sales Volume", "Simulated Sales", "Adjusted Actual Sales", "Estimated Sales (Theory)", "Estimated Sales (Category Curve)", "Adjusted Sales (Full Logic)", "Final Estimated Sales", "Daily Profit", "ROI %", "Offer Count", "Review Count", "Error - Theory vs Actual", "Error - Full Logic vs Actual", "Error - Category vs Actual", "Accuracy - Theory", "Accuracy - Full Logic", "Accuracy - Category"]
df_all["BSR Band"] = df_all["BSR Band"].cat.add_categories(["Invalid"]).fillna("Invalid")

# Summary by BSR Band
df_all_summary = df_all.groupby("BSR Band", observed=True)[summary_cols].mean(numeric_only=True).reset_index()
df_all_summary.to_csv(os.path.join(output_folder, "bsr_band_summary.csv"), index=False)

# Summary by Category and BSR Band
if "Category" in df_all.columns:
    df_all["BSR Band"] = pd.Categorical(df_all["BSR Band"], categories=bsr_labels + ["Invalid"], ordered=True)
    df_categorised = df_all.groupby(["Category", "BSR Band"], observed=True)[summary_cols].mean(numeric_only=True).reset_index()
    df_categorised.to_csv(os.path.join(output_folder, "bsr_band_summary_by_category.csv"), index=False)
    print("✅ Category-level summary saved to combined_exports/bsr_band_summary_by_category.csv")
else:
    print("⚠️ 'Category' column missing, category summary skipped.")

# Summary by Source and BSR Band
source_column = None
if "Chosen Source" in df_all.columns:
    source_column = "Chosen Source"
elif "Source" in df_all.columns:
    source_column = "Source"
    df_all["Chosen Source"] = df_all["Source"]
    print("ℹ️ Using 'Source' column as 'Chosen Source'")

if source_column:
    df_all["BSR Band"] = pd.Categorical(df_all["BSR Band"], categories=bsr_labels + ["Invalid"], ordered=True)
    df_by_source = df_all.groupby(["Chosen Source", "BSR Band"], observed=True)[summary_cols].mean(numeric_only=True).reset_index()
    df_by_source.to_csv(os.path.join(output_folder, "bsr_band_summary_by_source.csv"), index=False)
    print("✅ Source-level summary saved to combined_exports/bsr_band_summary_by_source.csv")
else:
    print("⚠️ No valid 'Chosen Source' or 'Source' column found, source summary skipped.")

# Summary by Offer Count Bucket and BSR Band
df_offer_bucket = df_all.groupby(["Offer Count Bucket", "BSR Band"], observed=True)[summary_cols].mean(numeric_only=True).reset_index()
df_offer_bucket.to_csv(os.path.join(output_folder, "bsr_band_summary_by_offer_count.csv"), index=False)
print("✅ Offer Count bucket summary saved to combined_exports/bsr_band_summary_by_offer_count.csv")

# Filtered summaries for ROI >= 10%
df_above_roi = df_all[df_all["Below Min ROI (10%)"] == False]

# Summary by BSR Band for above 10% ROI
summary_above_roi = df_above_roi.groupby("BSR Band", observed=True)[summary_cols].mean(numeric_only=True).reset_index()
summary_above_roi.to_csv(os.path.join(output_folder, "bsr_band_summary_above_roi.csv"), index=False)

# Summary by Category and BSR Band for above 10% ROI
if "Category" in df_above_roi.columns:
    summary_above_roi_by_cat = df_above_roi.groupby(["Category", "BSR Band"], observed=True)[summary_cols].mean(numeric_only=True).reset_index()
    summary_above_roi_by_cat.to_csv(os.path.join(output_folder, "bsr_band_summary_by_category_above_roi.csv"), index=False)
    print("✅ ROI>10% category-level summary saved to combined_exports/bsr_band_summary_by_category_above_roi.csv")

# Summary by Source and BSR Band for above 10% ROI
if source_column:
    summary_above_roi_by_source = df_above_roi.groupby(["Chosen Source", "BSR Band"], observed=True)[summary_cols].mean(numeric_only=True).reset_index()
    summary_above_roi_by_source.to_csv(os.path.join(output_folder, "bsr_band_summary_by_source_above_roi.csv"), index=False)
    print("✅ ROI>10% source-level summary saved to combined_exports/bsr_band_summary_by_source_above_roi.csv")

import pandas as pd
import charset_normalizer as chardet   # drop-in replacement
from pathlib import Path
import io

input_file  = "Enhanced-GB.tsv"
output_file = "Enhanced-GB.xlsx"

columns = [
    "Product ID", "SKU", "Brand", "Description", "Cost Price", "Selling Price",
    "Currency", "Timestamp", "Stock Level", "Stock Date", "Category Code",
    "Availability", "Category Description", "End User", "EAN", "Special",
    "Department", "Subcategory", "Restricted", "Weight (kg)"
]

# --- Read with correct encoding -------------------------------------------
raw = Path(input_file).read_bytes()
enc = chardet.detect(raw)["encoding"] or "latin1"
df = pd.read_csv(io.BytesIO(raw), sep="\t", header=None,
                 names=columns, dtype=str, encoding=enc)

# --- Type conversions ------------------------------------------------------
for col in ["Cost Price", "Selling Price", "Stock Level", "Weight (kg)"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# --- Write -----------------------------------------------------------------
df.to_excel(output_file, index=False)
print(f"TSV '{input_file}' → Excel '{output_file}' (encoding detected as {enc}).")

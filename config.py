"""Configuration for local PySpark runs and future Glue deployment."""

import os
from pathlib import Path

# Local input path; override via environment or CLI.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INPUT_PATH = os.environ.get(
    "PHARMA_INPUT_PATH",
    str(PROJECT_ROOT / "data" / "indian_pharmaceutical_products_raw.csv"),
)
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "output"

# Placeholder values treated as null
NULL_PLACEHOLDERS = {"", "NA", "N/A", "na", "n/a", "NULL", "null", "None", "none", "-"}

# Dosage form standardization map
DOSAGE_FORM_MAP = {
    "tablet": "tablet",
    "tab": "tablet",
    "tablets": "tablet",
    "capsule": "capsule",
    "caps": "capsule",
    "capsules": "capsule",
    "syrup": "syrup",
    "suspension": "suspension",
    "cream": "cream",
    "ointment": "ointment",
    "injection": "injection",
    "drops": "drops",
    "gel": "gel",
    "lotion": "lotion",
    "inhaler": "inhaler",
    "patch": "patch",
}

# Final curated output columns
FINAL_COLUMNS = [
    "product_id",
    "product_name_clean",
    "manufacturer_clean",
    "ingredient_clean",
    "strength_value",
    "strength_unit",
    "dosage_form",
    "package_quantity",
    "price",
    "price_per_unit",
    "price_band",
    "manufacturer_product_rank",
    "quality_flag",
    "therapeutic_class",
    "is_combination",
    "price_vs_manufacturer_avg_pct",
    "price_vs_category_avg_pct",
]

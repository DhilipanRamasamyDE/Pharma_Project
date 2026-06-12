"""Medicine-name processing: parse brand, strength, dosage form; flag combinations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config import DOSAGE_FORM_MAP


def _dosage_form_expr():
    raw = F.lower(F.trim(F.col("dosage_form")))
    mapped = F.lit("other")
    for key, value in DOSAGE_FORM_MAP.items():
        mapped = F.when(raw == key, F.lit(value)).otherwise(mapped)
    # Also extract from brand name if dosage_form is missing
    from_brand = F.regexp_extract(F.lower(F.col("brand_name")), r"(tablet|capsule|syrup|cream|suspension|injection|drops|gel|ointment)", 1)
    return F.coalesce(
        F.when(mapped != "other", mapped),
        from_brand,
        F.lit("other"),
    )


def parse_medicine_fields(df: DataFrame) -> DataFrame:
    """Separate brand name, strength, and dosage form from product name."""
    # Extract strength value and unit from primary_strength or brand name
    strength_source = F.coalesce(F.col("primary_strength"), F.col("brand_name"))
    strength_value = F.expr(
        "try_cast(regexp_extract(coalesce(primary_strength, brand_name), r'([\\d.]+)', 1) as double)"
    )
    strength_unit_raw = F.regexp_extract(strength_source, r"([\d.]+)\s*([a-zA-Z%/]+)", 2)

    # Normalize units: mcg, mg, ml, g, %
    strength_unit = (
        F.when(
            F.lower(strength_unit_raw).rlike(r"^mcg|ug|" + "\u03bcg"),
            F.lit("mcg"),
        )
        .when(F.lower(strength_unit_raw).rlike(r"^mg"), F.lit("mg"))
        .when(F.lower(strength_unit_raw).rlike(r"^ml"), F.lit("ml"))
        .when(F.lower(strength_unit_raw).rlike(r"^gm?$"), F.lit("g"))
        .when(strength_unit_raw.rlike(r"%"), F.lit("percent"))
        .otherwise(F.lower(strength_unit_raw))
    )

    # Brand name without strength/dosage suffixes
    product_name_clean = F.trim(
        F.regexp_replace(
            F.regexp_replace(
                F.col("brand_name"),
                r"(?i)\s*\d+(\.\d+)?\s*(mg|mcg|ml|g|%|iu).*$",
                "",
            ),
            r"(?i)\s+(tablet|tab|tablets|capsule|caps|syrup|cream|suspension|injection|drops|gel|ointment|sr|dx|ls|duo).*$",
            "",
        )
    )

    manufacturer_clean = F.regexp_replace(F.trim(F.col("manufacturer")), r"\s+", " ")
    ingredient_clean = F.trim(F.col("primary_ingredient"))

    is_combination = F.col("num_active_ingredients") > 1

    return (
        df.withColumn("product_name_clean", product_name_clean)
        .withColumn("manufacturer_clean", manufacturer_clean)
        .withColumn("ingredient_clean", ingredient_clean)
        .withColumn("strength_value", strength_value)
        .withColumn("strength_unit", strength_unit)
        .withColumn("dosage_form", _dosage_form_expr())
        .withColumn("package_quantity", F.col("pack_size"))
        .withColumn("is_combination", is_combination)
    )

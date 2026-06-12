"""Quality checks: impossible values, duplicate names, strength validation."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_quality_flags(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "quality_flag",
        F.when(
            F.col("product_name_clean").isNull() | (F.trim(F.col("product_name_clean")) == ""),
            F.lit("missing_product_name"),
        )
        .when(
            F.col("price").isNull() | (F.col("price") <= 0) | (F.col("price") > 100_000),
            F.lit("invalid_price"),
        )
        .when(
            F.col("dosage_form").isin("tablet", "capsule") & F.col("strength_value").isNull(),
            F.lit("missing_strength"),
        )
        .when(
            F.col("strength_value").isNotNull() & F.col("strength_unit").isNull(),
            F.lit("missing_strength_unit"),
        )
        .otherwise(F.lit("ok")),
    )


def duplicate_names_different_manufacturers(df: DataFrame) -> DataFrame:
    """Products with same cleaned name sold by different manufacturers."""
    name_mfr = df.groupBy("product_name_clean").agg(
        F.countDistinct("manufacturer_clean").alias("manufacturer_count"),
        F.collect_set("manufacturer_clean").alias("manufacturers"),
    )
    return (
        name_mfr.filter(F.col("manufacturer_count") > 1)
        .orderBy(F.desc("manufacturer_count"))
    )


def run_quality_report(df: DataFrame) -> None:
    print("\n=== QUALITY FLAG DISTRIBUTION ===")
    df.groupBy("quality_flag").count().orderBy(F.desc("count")).show(20, truncate=False)

    print("\n=== DUPLICATE PRODUCT NAMES (DIFFERENT MANUFACTURERS) - top 10 ===")
    duplicate_names_different_manufacturers(df).show(10, truncate=False)

    print("\n=== INVALID PRICE SAMPLES ===")
    df.filter(F.col("quality_flag") == "invalid_price").select(
        "product_id", "product_name_clean", "price", "manufacturer_clean"
    ).show(10, truncate=False)

"""Price transformations: extract, per-unit, bands, vs averages."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_prices(df: DataFrame) -> DataFrame:
    # Price is already numeric; extract from packaging_raw as fallback validation
    price_from_text = F.expr("try_cast(regexp_extract(packaging_raw, r'([\\d.]+)', 1) as double)")
    price = F.coalesce(F.col("price_inr"), price_from_text)

    # Price per tablet/capsule or per ml for liquids
    unit_divisor = (
        F.when(F.col("dosage_form").isin("tablet", "capsule"), F.col("package_quantity"))
        .when(F.col("dosage_form").isin("syrup", "suspension"), F.col("package_quantity"))
        .otherwise(F.col("package_quantity"))
    )
    price_per_unit = F.when(
        unit_divisor > 0,
        F.try_divide(price, unit_divisor),
    ).otherwise(F.lit(None))

    df = (
        df.withColumn("price", price)
        .withColumn("price_per_unit", price_per_unit)
    )

    # Compute tertile boundaries
    boundaries = df.approxQuantile("price", [0.33, 0.66], 0.01)
    low_cut, high_cut = boundaries[0], boundaries[1]

    df = df.withColumn(
        "price_band",
        F.when(F.col("price") <= low_cut, F.lit("Low"))
        .when(F.col("price") <= high_cut, F.lit("Medium"))
        .otherwise(F.lit("High")),
    )

    # Compare with manufacturer and category averages
    mfr_avg = df.groupBy("manufacturer_clean").agg(F.avg("price").alias("mfr_avg_price"))
    cat_avg = df.groupBy("therapeutic_class").agg(F.avg("price").alias("category_avg_price"))

    df = (
        df.join(mfr_avg, on="manufacturer_clean", how="left")
        .join(cat_avg, on="therapeutic_class", how="left")
        .withColumn(
            "price_vs_manufacturer_avg_pct",
            F.round(
                F.try_divide(
                    F.col("price") - F.col("mfr_avg_price"),
                    F.col("mfr_avg_price"),
                )
                * 100,
                2,
            ),
        )
        .withColumn(
            "price_vs_category_avg_pct",
            F.round(
                F.try_divide(
                    F.col("price") - F.col("category_avg_price"),
                    F.col("category_avg_price"),
                )
                * 100,
                2,
            ),
        )
        .drop("mfr_avg_price", "category_avg_price")
    )

    return df

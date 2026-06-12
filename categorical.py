"""Categorical transformations and manufacturer analysis."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def add_categorical_features(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("therapeutic_class", F.lower(F.trim(F.col("therapeutic_class"))))
        .withColumn("is_discontinued_flag", F.col("is_discontinued").cast("int"))
        .withColumn(
            "is_liquid_form",
            F.col("dosage_form").isin("syrup", "suspension", "drops", "injection").cast("int"),
        )
        .withColumn(
            "is_topical_form",
            F.col("dosage_form").isin("cream", "ointment", "gel", "lotion").cast("int"),
        )
        .withColumn(
            "is_oral_solid_form",
            F.col("dosage_form").isin("tablet", "capsule").cast("int"),
        )
    )


def manufacturer_summary(df: DataFrame) -> DataFrame:
    """Count products by manufacturer and rank by count / avg price."""
    count_window = Window.orderBy(F.desc("product_count"))
    price_window = Window.orderBy(F.desc("avg_price"))

    return (
        df.groupBy("manufacturer_clean")
        .agg(
            F.count("*").alias("product_count"),
            F.round(F.avg("price"), 2).alias("avg_price"),
            F.round(F.expr("percentile_approx(price, 0.5)"), 2).alias("median_price"),
            F.max("price").alias("max_price"),
        )
        .withColumn("rank_by_count", F.row_number().over(count_window))
        .withColumn("rank_by_avg_price", F.row_number().over(price_window))
        .orderBy(F.desc("product_count"))
    )


def expensive_manufacturers(df: DataFrame, threshold_pct: float = 50.0) -> DataFrame:
    """Manufacturers with products priced >50% above their own average."""
    mfr_avg = df.groupBy("manufacturer_clean").agg(F.avg("price").alias("mfr_avg"))
    return (
        df.join(mfr_avg, on="manufacturer_clean")
        .withColumn(
            "pct_above_avg",
            F.try_divide(F.col("price") - F.col("mfr_avg"), F.col("mfr_avg")) * 100,
        )
        .filter(F.col("pct_above_avg") > threshold_pct)
        .select("manufacturer_clean", "product_name_clean", "price", "mfr_avg", "pct_above_avg")
        .orderBy(F.desc("pct_above_avg"))
    )

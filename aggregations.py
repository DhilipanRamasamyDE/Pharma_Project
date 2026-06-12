"""Aggregations and pivoting exercises."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def category_aggregations(df: DataFrame) -> DataFrame:
    """Product count, average price, and median price by therapeutic category."""
    return (
        df.groupBy("therapeutic_class")
        .agg(
            F.count("*").alias("product_count"),
            F.round(F.avg("price"), 2).alias("avg_price"),
            F.round(F.expr("percentile_approx(price, 0.5)"), 2).alias("median_price"),
            F.round(F.min("price"), 2).alias("min_price"),
            F.round(F.max("price"), 2).alias("max_price"),
        )
        .orderBy(F.desc("product_count"))
    )


def pivot_dosage_forms(df: DataFrame) -> DataFrame:
    """Pivot dosage forms into separate count columns per manufacturer."""
    return (
        df.groupBy("manufacturer_clean")
        .pivot("dosage_form")
        .agg(F.count("product_id"))
        .na.fill(0)
    )


def most_common_ingredient_per_manufacturer(df: DataFrame) -> DataFrame:
    """Find the most common primary ingredient for each manufacturer."""
    ingredient_counts = (
        df.filter(F.col("ingredient_clean").isNotNull())
        .groupBy("manufacturer_clean", "ingredient_clean")
        .agg(F.count("*").alias("ingredient_count"))
    )
    window = Window.partitionBy("manufacturer_clean").orderBy(F.desc("ingredient_count"))
    return (
        ingredient_counts.withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") == 1)
        .select("manufacturer_clean", "ingredient_clean", "ingredient_count")
        .orderBy(F.desc("ingredient_count"))
    )

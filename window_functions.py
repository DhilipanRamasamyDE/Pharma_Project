"""Window-function exercises: rank, min/max per group, running totals, lag."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def apply_window_functions(df: DataFrame) -> DataFrame:
    # Rank products by price within each manufacturer
    mfr_window = Window.partitionBy("manufacturer_clean").orderBy(F.col("price").desc())
    df = df.withColumn("manufacturer_product_rank", F.rank().over(mfr_window))

    # Cheapest and most expensive per therapeutic category
    cat_price_window = Window.partitionBy("therapeutic_class")
    df = (
        df.withColumn("category_min_price", F.min("price").over(cat_price_window))
        .withColumn("category_max_price", F.max("price").over(cat_price_window))
        .withColumn("is_cheapest_in_category", F.col("price") == F.col("category_min_price"))
        .withColumn("is_most_expensive_in_category", F.col("price") == F.col("category_max_price"))
    )

    # Running total and percentage contribution by manufacturer
    mfr_running_window = (
        Window.partitionBy("manufacturer_clean")
        .orderBy(F.col("price").desc())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    mfr_total_window = Window.partitionBy("manufacturer_clean")

    df = (
        df.withColumn("running_price_total", F.sum("price").over(mfr_running_window))
        .withColumn("manufacturer_total_revenue_proxy", F.sum("price").over(mfr_total_window))
        .withColumn(
            "pct_contribution_within_manufacturer",
            F.round(
                F.try_divide(
                    F.col("price"),
                    F.col("manufacturer_total_revenue_proxy"),
                )
                * 100,
                4,
            ),
        )
    )

    # Lag: compare with previous product price within manufacturer (ordered by price)
    df = df.withColumn("prev_product_price", F.lag("price", 1).over(mfr_window))
    df = df.withColumn(
        "price_change_from_prev",
        F.round(F.col("price") - F.col("prev_product_price"), 2),
    )

    return df

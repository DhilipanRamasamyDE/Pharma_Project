"""Data profiling: schema, counts, nulls, duplicates, descriptive stats."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def profile_schema(df: DataFrame) -> None:
    print("\n=== SCHEMA ===")
    df.printSchema()


def profile_row_count(df: DataFrame) -> int:
    count = df.count()
    print(f"\n=== ROW COUNT: {count:,} ===")
    return count


def profile_nulls(df: DataFrame) -> DataFrame:
    print("\n=== NULL COUNTS ===")
    null_exprs = [
        F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""), 1).otherwise(0)).alias(c)
        for c in df.columns
    ]
    null_df = df.agg(*null_exprs)
    null_df.show(vertical=True, truncate=False)
    return null_df


def profile_duplicates(df: DataFrame, key_cols: list[str]) -> None:
    print(f"\n=== DUPLICATES on {key_cols} ===")
    total = df.count()
    distinct = df.select(*key_cols).distinct().count()
    print(f"Total rows: {total:,} | Distinct keys: {distinct:,} | Duplicate rows: {total - distinct:,}")


def profile_distinct_values(df: DataFrame, columns: list[str], limit: int = 15) -> None:
    print("\n=== DISTINCT VALUE COUNTS ===")
    for col_name in columns:
        n = df.select(col_name).distinct().count()
        print(f"  {col_name}: {n:,} distinct values")
        df.groupBy(col_name).count().orderBy(F.desc("count")).show(limit, truncate=False)


def profile_numeric_stats(df: DataFrame, numeric_cols: list[str]) -> DataFrame:
    print("\n=== NUMERIC DESCRIPTIVE STATISTICS ===")
    stats_exprs = []
    for col_name in numeric_cols:
        if col_name in df.columns:
            stats_exprs.extend(
                [
                    F.min(col_name).alias(f"{col_name}_min"),
                    F.max(col_name).alias(f"{col_name}_max"),
                    F.avg(col_name).alias(f"{col_name}_avg"),
                    F.stddev(col_name).alias(f"{col_name}_stddev"),
                    F.expr(f"percentile_approx({col_name}, 0.5)").alias(f"{col_name}_median"),
                ]
            )
    if stats_exprs:
        stats_df = df.agg(*stats_exprs)
        stats_df.show(vertical=True, truncate=False)
        return stats_df
    return df.limit(0)


def run_profiling(df: DataFrame) -> None:
    """Run full profiling suite on raw or cleaned data."""
    profile_schema(df)
    profile_row_count(df)
    profile_nulls(df)
    profile_duplicates(df, ["product_id"])
    profile_duplicates(df, ["brand_name", "manufacturer", "pack_size"])
    profile_distinct_values(df, ["manufacturer", "dosage_form", "therapeutic_class", "primary_ingredient"])
    profile_numeric_stats(df, ["price_inr", "pack_size", "num_active_ingredients"])

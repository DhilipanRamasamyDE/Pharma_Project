"""Data cleaning: snake_case, trim, null placeholders, type casting, dedup."""

import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType

from src.config import NULL_PLACEHOLDERS


def _to_snake_case(name: str) -> str:
    name = re.sub(r"[^\w]+", "_", name.strip())
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.lower().strip("_")


def standardize_column_names(df: DataFrame) -> DataFrame:
    for col_name in df.columns:
        snake = _to_snake_case(col_name)
        if snake != col_name:
            df = df.withColumnRenamed(col_name, snake)
    return df


def _null_if_placeholder(col_expr):
    """Convert empty strings and known placeholders to null."""
    expr = F.trim(col_expr.cast("string"))
    condition = expr.isNull() | (expr == "")
    for placeholder in NULL_PLACEHOLDERS:
        condition = condition | (F.lower(expr) == placeholder.lower())
    return F.when(condition, F.lit(None)).otherwise(expr)


def clean_string_columns(df: DataFrame, string_cols: list[str]) -> DataFrame:
    for col_name in string_cols:
        if col_name in df.columns:
            cleaned = _null_if_placeholder(F.col(col_name))
            # Title-case manufacturer and ingredient names; preserve brand casing mostly
            if col_name in ("manufacturer", "manufacturer_raw", "primary_ingredient"):
                cleaned = F.initcap(cleaned)
            df = df.withColumn(col_name, cleaned)
    return df


def cast_types(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("price_inr", F.col("price_inr").cast(DoubleType()))
        .withColumn("pack_size", F.col("pack_size").cast(DoubleType()))
        .withColumn("num_active_ingredients", F.col("num_active_ingredients").cast(IntegerType()))
        .withColumn(
            "is_discontinued",
            F.when(F.lower(F.col("is_discontinued").cast("string")).isin("true", "1", "yes"), True)
            .when(F.lower(F.col("is_discontinued").cast("string")).isin("false", "0", "no"), False)
            .otherwise(None)
            .cast(BooleanType()),
        )
        .withColumn("product_id", F.col("product_id").cast(IntegerType()))
    )


def remove_exact_duplicates(df: DataFrame) -> DataFrame:
    return df.dropDuplicates()


def remove_business_key_duplicates(df: DataFrame) -> DataFrame:
    """Keep first occurrence per brand + manufacturer + pack_size business key."""
    from pyspark.sql.window import Window

    window = Window.partitionBy("brand_name", "manufacturer", "pack_size").orderBy(F.col("product_id").asc())
    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def clean_data(df: DataFrame) -> DataFrame:
    df = standardize_column_names(df)
    string_cols = [c for c, t in df.dtypes if t == "string"]
    df = clean_string_columns(df, string_cols)
    df = cast_types(df)
    df = remove_exact_duplicates(df)
    df = remove_business_key_duplicates(df)
    return df

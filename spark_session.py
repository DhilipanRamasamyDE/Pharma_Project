"""Create a local SparkSession for PyCharm testing."""

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parent.parent
HADOOP_HOME = PROJECT_ROOT / "hadoop"


def _configure_windows_hadoop() -> None:
    """Spark on Windows needs HADOOP_HOME with winutils.exe to write files."""
    if os.name != "nt":
        return

    bin_dir = HADOOP_HOME / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    os.environ["HADOOP_HOME"] = str(HADOOP_HOME)
    os.environ["hadoop.home.dir"] = str(HADOOP_HOME)


def create_local_spark(app_name: str = "pharma-products-local") -> SparkSession:
    _configure_windows_hadoop()

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
    )

    if os.name == "nt":
        builder = builder.config("spark.hadoop.hadoop.home.dir", str(HADOOP_HOME))

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    return builder.getOrCreate()

"""
AWS Glue job stub; deploy after local testing passes.

Glue job parameters (set in Glue console or workflow):
  --input_path   s3://bucket/raw/indian_pharmaceutical_products_raw.csv
  --output_path  s3://bucket/curated/pharma_products/
"""

import sys
from pathlib import Path

# Glue injects these at runtime
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# Add src to path when scripts are uploaded as extra files
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipeline import run_pipeline


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_path", "output_path"])

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    run_pipeline(
        spark=spark,
        input_path=args["input_path"],
        output_dir=Path(args["output_path"]),
        profile=False,
        local_mode=False,
    )

    job.commit()


if __name__ == "__main__":
    main()

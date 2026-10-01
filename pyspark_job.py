from pyspark.sql import SparkSession, functions as F
import sys


def clean_data(df):

    cleaned_df = df.filter(F.col("amount") > 0)
    cleaned_df = cleaned_df.filter(F.col("name").isNotNull())

    return (
        cleaned_df.withColumn("amount_with_tax", F.col("amount") * 1.20)
    )

def main(csv_path):
    spark = (
        SparkSession.builder
        .appName("SparkProject")
        .getOrCreate()
    )

    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
        cleaned_data = clean_data(df)

        cleaned_data.show(truncate=False)

        return clean_data
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1])
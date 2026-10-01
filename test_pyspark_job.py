import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark_job import clean_data


@pytest.fixture(scope="session")
def spark():
    session = ( SparkSession.builder
            .appName("SparkProject")
            .getOrCreate())
    
    yield session
    session.stop()



def test_data_does_not_contain_negatives_or_zeroes(spark):
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("amount", DoubleType(), True),
    ])

    sample_data = [
        {"id": 1, "name": "Alice", "amount": 100.0},   # valid
        {"id": 2, "name": "Bob",   "amount": 0.0},     # boundary, must be removed
        {"id": 3, "name": "Carol", "amount": -20.0},   # negative, must be removed
        {"id": 4, "name": "David", "amount": 0.01},    # tiny positive, must be kept
        {"id": 5, "name": "Eve",   "amount": None},    # null amount, see below
    ]

    df = spark.createDataFrame(sample_data, schema)

    transformed_df = clean_data(df)

    expected_data = {1, 4}
    actual_data = {row.id for row in transformed_df.collect()}

    assert actual_data == expected_data


def test_amount_with_tax_is_calculated_correctly(spark):
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("amount", DoubleType(), True),
    ])

    sample_data = [
        {"id": 1, "name": "Alice", "amount": 100.0},
        {"id": 2, "name": "Bob",   "amount": 250.5},
        {"id": 3, "name": "Carol", "amount": 19.99},
        {"id": 4, "name": "David", "amount": 0.01},
    ]

    df = spark.createDataFrame(sample_data, schema)

    result = clean_data(df)

    actual = {row.id: row.amount_with_tax for row in result.collect()}

    assert actual[1] == pytest.approx(120.0)
    assert actual[2] == pytest.approx(300.6)
    assert actual[3] == pytest.approx(23.988)
    assert actual[4] == pytest.approx(0.012)
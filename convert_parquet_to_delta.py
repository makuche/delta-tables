from delta.tables import DeltaTable
from io_ import _init_spark, write_data_to_spark_dataframe

spark = _init_spark()

PARQUET_PATH = 'data/insert'

# Create dummy dataframe and save as Parquet
data = spark.createDataFrame(
    [(77, 'ab'), (88, 'cb'), 
     (99, 'cd')], ["id", "value"])
write_data_to_spark_dataframe(
    data, PARQUET_PATH)

# Converts the parquet file in-place
deltaTable = DeltaTable.convertToDelta(
    spark, f"parquet.`{PARQUET_PATH}`")
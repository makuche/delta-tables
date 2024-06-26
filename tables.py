import os
import shutil
import pyspark
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import pandas as pd

TMP_TABLE_PATH = '/tmp/table'

def _remove_table_if_exists(path):
    if os.path.exists(path):
        shutil.rmtree(path)

def _init_spark():
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", 
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    return configure_spark_with_delta_pip(builder).getOrCreate()

def save_history_to_html(delta_table, filename):
    history_df = delta_table.history()
    history_html = history_df.toPandas().to_html()
    with open(filename, "w") as file:
        file.write(history_html)

def write_data_to_delta_table(data, path, mode="overwrite"):
    data.write.format("delta").mode(mode).save(path)

def main():
    _remove_table_if_exists(TMP_TABLE_PATH)
    spark = _init_spark()

    # Initial data write
    data = spark.range(0, 5)
    write_data_to_delta_table(data, TMP_TABLE_PATH)
    delta_table = DeltaTable.forPath(spark, TMP_TABLE_PATH)
    save_history_to_html(delta_table, "history.html")

    # Append new data
    new_data = spark.range(5, 20)
    write_data_to_delta_table(new_data, TMP_TABLE_PATH, mode="append")
    save_history_to_html(delta_table, "updated_history.html")

    # Overwrite with new data
    overwrite_data = spark.range(10, 12)
    write_data_to_delta_table(overwrite_data, TMP_TABLE_PATH, mode="overwrite")
    save_history_to_html(delta_table, "final_history.html")

    spark.stop()

if __name__ == "__main__":
    main()

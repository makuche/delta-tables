import os
import shutil
import pyspark
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
import argparse

TMP_TABLE_PATH = '/tmp/table'

def _remove_table_if_exists(path: str) -> None:
    if os.path.exists(path):
        shutil.rmtree(path)

def _init_spark() -> SparkSession:
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", 
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    return configure_spark_with_delta_pip(builder).getOrCreate()

def save_history_to_html(delta_table: DeltaTable, filename: str) -> None:
    history_df = delta_table.history()
    history_html = history_df.toPandas().to_html()
    with open(filename, "w") as file:
        file.write(history_html)

def write_data_to_delta_table(data: DataFrame, path: str, mode: str = "overwrite") -> None:
    data.write.format("delta").mode(mode).save(path)

def print_table(delta_table: DeltaTable) -> None:
    df = delta_table.toDF().orderBy("id").show()

def main(print_tables: bool) -> None:
    _remove_table_if_exists(TMP_TABLE_PATH)
    spark = _init_spark()

    data = spark.createDataFrame([(0, 'a'), (1, 'b'), (2, 'c')], ["id", "value"])
    write_data_to_delta_table(data, TMP_TABLE_PATH)
    delta_table = DeltaTable.forPath(spark, TMP_TABLE_PATH)
    if print_tables:
        print_table(delta_table)

    new_data = spark.createDataFrame([(3, 'd'), (4, 'e')], ["id", "value"])
    write_data_to_delta_table(new_data, TMP_TABLE_PATH, mode="append")
    if print_tables:
        print_table(delta_table)

    upsert_data = spark.createDataFrame([(2, 'z'), (5, 'f')], ["id", "value"])
    delta_table.alias("target").merge(
        upsert_data.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    if print_tables:
        print_table(delta_table)

    update_data = spark.createDataFrame([(1, 'y')], ["id", "value"])
    delta_table.alias("target").merge(
        update_data.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdateAll().execute()
    if print_tables:
        print_table(delta_table)

    delta_table.delete("id = 0")
    if print_tables:
        print_table(delta_table)

    overwrite_data = spark.createDataFrame([(10, 'x'), (11, 'w')], ["id", "value"])
    write_data_to_delta_table(overwrite_data, TMP_TABLE_PATH, mode="overwrite")
    if print_tables:
        print_table(delta_table)

    save_history_to_html(delta_table, "history.html")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Delta Table Operations')
    parser.add_argument(
        '--print-tables', action='store_true', help='Print tables after each operation')
    args = parser.parse_args()

    main(args.print_tables)

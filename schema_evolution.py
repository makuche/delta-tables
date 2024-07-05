
import os
import shutil
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from io_ import save_history_to_html, _init_spark, write_data_to_delta_table, save_df_to_html

TABLE_PATH: str = '/tmp/schema_change_table'
spark: SparkSession = _init_spark()

def remove_table(path: str) -> None:
    if os.path.exists(path):
        print("Removing the following path: ", path)
        shutil.rmtree(path)

def save_history_to_html(delta_table: DeltaTable, filename: str) -> None:
    history_df: DataFrame = delta_table.history()
    save_df_to_html(history_df, filename)

def init_dummy_data(path_substring: str) -> str:
    path: str = TABLE_PATH + path_substring
    remove_table(path)
    data: DataFrame = spark.createDataFrame([(0, 'a'), (1, 'b'), (2, 'c')], ["id", "value"])
    write_data_to_delta_table(data, path)
    return path

def print_table_and_schema(path: str, mode: str) -> None:
    if mode == "Init":
        print("Initial table and schema:")
    elif mode == "Changed":
        print("Changed table and schema:")
    df: DataFrame = spark.read.format('delta').load(path)
    df.show()
    df.printSchema()

def test_overwrite_schema(path_substring: str) -> None:
    path: str = init_dummy_data(path_substring)
    print_table_and_schema(path, mode="Init")
    data_updated: DataFrame = spark.createDataFrame([(3, 'a', 11), (4, 'b', 22)], ["id", "value", "new_col"])
    data_updated.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(path)
    delta_table: DeltaTable = DeltaTable.forPath(spark, path)
    save_history_to_html(delta_table, 'schema_change_overwrite.html')
    print_table_and_schema(path, mode="Changed")

def test_merge_schema(path_substring: str) -> None:
    path: str = init_dummy_data(path_substring)
    print_table_and_schema(path, mode="Init")
    data_updated: DataFrame = spark.createDataFrame([(3, 'a', 11), (4, 'b', 22)], ["id", "value", "new_col"])
    data_updated.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(path)
    delta_table: DeltaTable = DeltaTable.forPath(spark, path)
    save_history_to_html(delta_table, 'schema_change_merge.html')
    print_table_and_schema(path, mode="Changed")

def test_change_column_name(path_substring: str) -> None:
    path: str = init_dummy_data(path_substring)
    print_table_and_schema(path, mode="Init")
    df: DataFrame = spark.read.format('delta').load(path)
    df = df.withColumnRenamed("value", "new_value")
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(path)
    delta_table: DeltaTable = DeltaTable.forPath(spark, path)
    save_history_to_html(delta_table, 'schema_change_rename.html')
    print_table_and_schema(path, mode="Changed")

def test_change_column_type(path_substring: str) -> None:
    path: str = init_dummy_data(path_substring)
    print_table_and_schema(path, mode="Init")
    df: DataFrame = spark.read.format('delta').load(path)
    df = df.withColumn("value", df["value"].cast("integer"))
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(path)
    delta_table: DeltaTable = DeltaTable.forPath(spark, path)
    save_history_to_html(delta_table, 'schema_change_type.html')
    print_table_and_schema(path, mode="Changed")

def test_remove_column(path_substring: str) -> None:
    path: str = init_dummy_data(path_substring)
    print_table_and_schema(path, mode="Init")
    df: DataFrame = spark.read.format('delta').load(path)
    df = df.drop("value")
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(path)
    delta_table: DeltaTable = DeltaTable.forPath(spark, path)
    save_history_to_html(delta_table, 'schema_change_remove.html')
    print_table_and_schema(path, mode="Changed")

def test_add_remove_constraints(path_substring: str) -> None:
    path: str = init_dummy_data(path_substring)
    print_table_and_schema(path, mode="Init")
    df: DataFrame = spark.read.format('delta').load(path)
    # The following causes a column of NULLs, because of str->int cast
    df = df.withColumn("value", F.col("value").cast("integer").alias("value"))
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(path)
    delta_table: DeltaTable = DeltaTable.forPath(spark, path)
    save_history_to_html(delta_table, 'schema_change_constraints.html')
    print_table_and_schema(path, mode="Changed")

def main() -> None:
    print("Testing to overwrite schema...")
    test_overwrite_schema("/overwrite_test")
    print("Testing to merge schema...")
    test_merge_schema("/merge_test")
    print("Testing to rename col...")
    test_change_column_name("/rename_test")
    print("Testing typecast...")
    test_change_column_type("/type_test")
    print("Testing removing a column...")
    test_remove_column("/remove_test")
    print("Testing removing constraints...")
    test_add_remove_constraints("/constraints_test")

main()

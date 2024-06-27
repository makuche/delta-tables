from delta.tables import DeltaTable
import argparse
from io_ import _init_spark, save_history_to_html, print_delta_table, _remove_table_if_exists, \
    write_data_to_delta_table

TMP_TABLE_PATH = '/tmp/table'

def main(print_table: bool, reset_table: bool) -> None:
    if reset_table: _remove_table_if_exists(TMP_TABLE_PATH)
    spark = _init_spark()

    data = spark.createDataFrame([(0, 'a'), (1, 'b'), (2, 'c')], ["id", "value"])
    write_data_to_delta_table(data, TMP_TABLE_PATH)
    delta_table = DeltaTable.forPath(spark, TMP_TABLE_PATH)
    if print_table: print_delta_table(delta_table)

    new_data = spark.createDataFrame([(3, 'd'), (4, 'e')], ["id", "value"])
    write_data_to_delta_table(new_data, TMP_TABLE_PATH, mode="append")
    if print_table: print_delta_table(delta_table)

    upsert_data = spark.createDataFrame([(2, 'z'), (5, 'f')], ["id", "value"])
    delta_table.alias("target").merge(
        upsert_data.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    if print_table: print_delta_table(delta_table)

    update_data = spark.createDataFrame([(1, 'y')], ["id", "value"])
    delta_table.alias("target").merge(
        update_data.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdateAll().execute()
    if print_table: print_delta_table(delta_table)

    delta_table.delete("id = 0")
    if print_table: print_delta_table(delta_table)

    overwrite_data = spark.createDataFrame([(10, 'x'), (11, 'w')], ["id", "value"])
    write_data_to_delta_table(overwrite_data, TMP_TABLE_PATH, mode="overwrite")
    if print_table: print_delta_table(delta_table)

    save_history_to_html(delta_table, "history.html")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Delta Table Operations')
    parser.add_argument(
        '--print-table', action='store_true', help='Print tables after each operation')
    parser.add_argument(
        '--reset-table', action='store_true', help='Remove the table and its history.')
    args = parser.parse_args()
    main(args.print_table, args.reset_table)

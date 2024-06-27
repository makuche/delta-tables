import os 
import shutil
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from delta import configure_spark_with_delta_pip

def _init_spark() -> SparkSession:
    builder = SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", 
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    return configure_spark_with_delta_pip(builder).getOrCreate()

def _remove_table_if_exists(path: str) -> None:
    if os.path.exists(path):
        shutil.rmtree(path)

def save_df_to_html(df_spark: DataFrame, filename: str) -> None:
    df_html = df_spark.toPandas().to_html()
    with open(f'logs/{filename}', 'w') as file:
        file.write(df_html)

def save_history_to_html(delta_table: DeltaTable, filename: str) -> None:
    history_df = delta_table.history()
    save_df_to_html(history_df, filename)

def print_delta_table(delta_table: DeltaTable) -> None:
    delta_table.toDF().orderBy("id").show()

def write_data_to_delta_table(data: DataFrame, path: str, mode: str = "overwrite") -> None:
    data.write.format("delta").mode(mode).save(path)

def write_data_to_spark_dataframe(data: DataFrame, path: str, mode: str = "overwrite") -> None:
    data.write.format("parquet").mode(mode).save(path)
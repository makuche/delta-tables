'''
Remove files no longer referenced by a DeltaTable 
and older than the retention threshold by running the 
vacuum command on the table. 
The default retention threshold for the files is 7 days. To change this behavior, see Data retention.
'''
import os
from delta.tables import *
from delta import configure_spark_with_delta_pip

def _init_spark() -> SparkSession:
    builder = SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", 
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    return configure_spark_with_delta_pip(builder).getOrCreate()

def save_history_to_html(delta_table: DeltaTable, filename: str) -> None:
    history_df = delta_table.history()
    history_html = history_df.toPandas().to_html()
    with open(filename, "w") as file:
        file.write(history_html)


TABLE_PATH = '/tmp/table'
if not os.path.exists(TABLE_PATH): exit() 

spark = _init_spark()
deltaTable = DeltaTable.forPath(spark, TABLE_PATH)
# vacuum files not required by versions with 
# t > default retention period
deltaTable.vacuum()

save_history_to_html(deltaTable, "logs/vacuum.html")
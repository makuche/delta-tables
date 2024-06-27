import os
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from io_ import save_history_to_html, _init_spark

TABLE_PATH = '/tmp/table'
if not os.path.exists(TABLE_PATH): print("Table does not exist"); exit() 

spark = _init_spark()

df_spark = spark.read.format('delta').load(TABLE_PATH)
df_spark.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .option("path", TABLE_PATH) \
  .saveAsTable("table")

deltaTable = DeltaTable.forPath(spark, TABLE_PATH)
save_history_to_html(deltaTable, 'schema_change.html')

# Why not just delete the table?
    # - Overwriting is faster, no need for deletions
    # - Old version of table still exists, time travel is available
    # - Schema change is atomic. Concurrent queries can still read the table while operation is done
    # - If table is large, and schema change might fail, you still have Delta Lake ACID
    #   guarantees, i.e. the table is still in the previous state


# 1) Load the table
# 2) Extend withColumn with Nones
# 3) Overwrite the schema


# vacuum files not required by versions with 
# t > default retention period
#print(deltaTable.history().show())
#print(deltaTable.history(1).show())


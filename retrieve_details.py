
'''
Retrieve change history on a DeltaTable.
'''
import os
from delta.tables import *
from delta import configure_spark_with_delta_pip
from io_ import save_df_to_html, _init_spark

TABLE_PATH = '/tmp/table'
if not os.path.exists(TABLE_PATH): print("Table does not exist"); exit() 

spark = _init_spark()
deltaTable = DeltaTable.forPath(spark, TABLE_PATH)
# Retrieve details about Delta table
save_df_to_html(deltaTable.detail(), 'details.html')
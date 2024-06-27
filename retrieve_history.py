'''
Retrieve change history on a DeltaTable.
'''
import os
from delta.tables import *
from io_ import save_history_to_html, _init_spark

TABLE_PATH = '/tmp/table'
if not os.path.exists(TABLE_PATH): print("Table does not exist"); exit() 

spark = _init_spark()
deltaTable = DeltaTable.forPath(spark, TABLE_PATH)
# vacuum files not required by versions with 
# t > default retention period
print(deltaTable.history().show())
print(deltaTable.history(1).show())

save_history_to_html(deltaTable, 'history.html')

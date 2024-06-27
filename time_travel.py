'''
Retrieve change history on a DeltaTable.
'''
import os
from delta.tables import *
from io_ import _init_spark

TABLE_PATH = '/tmp/table'
if not os.path.exists(TABLE_PATH): print("Table does not exist"); exit() 

spark = _init_spark()
deltaTable = DeltaTable.forPath(spark, TABLE_PATH)
deltaTable.restoreToVersion(0) # oldest table version
# deltaTable.restoreToTimestamp('2024-06-27') # restore to a specific timestamp


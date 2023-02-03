import os
import pandas as pd
from db_connection import *
from file_tracker_status import *
con,cur=db_connection()


def Datainsert(valueCols={ValueCols}):
    file_check({KeyFile},'dimension')
    df_data=pd.read_csv("/processing_data/" + {KeyFile})
    {DatasetCasting}
    df_snap = df_data[valueCols]
    try:
         for index,row in df_snap.iterrows():
            values = []
            for i in valueCols:
              values.append(row[i])
            query = ''' INSERT INTO {TargetTable}({InputCols}) VALUES ({Values});'''\
            .format(','.join(map(str,values)))
            cur.execute(query)
         status_track({KeyFile}, 'dimension', 'Completed_{DimensionName}')

    except Exception as error:
        print(error)

Datainsert()


















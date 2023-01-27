import os
import pandas as pd
from db_connection import db_connection

con,cur=db_connection()

def Datainsert(valueCols={ValueCols}):
    df_data=pd.read_csv(os.path.dirname(os.path.abspath(__file__)) + "/events/" + {KeyFile})
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
    except Exception as error:
        print(error)

Datainsert()


















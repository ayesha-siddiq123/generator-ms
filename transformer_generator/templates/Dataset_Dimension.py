import os
import configparser
import pandas as pd
from urllib.parse import quote
from sqlalchemy import create_engine
from db import *
con,cur = db_connection()

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
            con.commit()
    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if con is not None:
            con.close()

Datainsert()


















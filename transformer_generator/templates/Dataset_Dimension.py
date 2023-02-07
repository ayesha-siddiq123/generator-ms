import pandas as pd
from db_connection import *
from file_tracker_status import *
from datetime import date

con,cur=db_connection()


def Datainsert(valueCols={ValueCols}):
    file_check('{KeyFile}','dimension')
    df_data=pd.read_csv(os.path.dirname(root_path)+"processing_data/{KeyFile}")
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
         status_track('{KeyFile}', 'dimension', 'Completed_{DimensionName}')

    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if con is not None:
            con.close()

Datainsert()


















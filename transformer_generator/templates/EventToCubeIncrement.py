import pandas as pd
from db_connection import *
from file_tracker_status import *
from datetime import date
con,cur=db_connection()

def aggTransformer(valueCols={ValueCols}):
    file_check('{KeyFile}','event')
    df_data = pd.read_csv(os.path.dirname(root_path)+"processing_data/{KeyFile}")
    {DatasetCasting}
    df_dimension = pd.read_sql('select {DimensionCols} from {DimensionTable}', con=con)
    event_dimension_merge = df_data.merge(df_dimension, on=['{MergeOnCol}'], how='inner')
    df_agg = event_dimension_merge.groupby({GroupBy}, as_index=False).agg({AggCols})
    col_list = df_agg.columns.to_list()
    df_snap = df_agg[col_list]
    try:
         for index,row in df_snap.iterrows():
            values = []
            for i in col_list:
              values.append(row[i])
            query = ''' INSERT INTO {TargetTable} As main_table({InputCols}) VALUES ({Values}) ON CONFLICT ({ConflictCols}) DO UPDATE SET {IncrementFormat};'''\
            .format(','.join(map(str,values)),{UpdateCol})
            cur.execute(query)
            con.commit()
         status_track('{KeyFile}', 'event', 'Completed_{DatasetName}')
    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if con is not None:
            con.close()

aggTransformer()


















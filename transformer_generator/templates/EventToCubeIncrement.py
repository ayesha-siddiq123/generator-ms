import os
import pandas as pd
from db_connection import db_connection

con,cur=db_connection()

def aggTransformer(valueCols={ValueCols}):

    df_events = pd.read_csv(os.path.dirname(os.path.abspath(__file__)) + "/events/" + {KeyFile})
    df_dimension = pd.read_sql('select {DimensionCols} from {DimensionTable}', con=con)
    event_dimension_merge = df_events.merge(df_dimension, on=['{MergeOnCol}'], how='inner')
    df_agg = event_dimension_merge.groupby({GroupBy}, as_index=False).agg({AggCols})
    {DatasetCasting}
    col_list = df_agg.columns.to_list()
    df_snap = df_agg[col_list]
    df_snap.columns = valueCols
    try:
         for index,row in df_snap.iterrows():
            values = []
            for i in valueCols:
              values.append(row[i])
            query = ''' INSERT INTO {TargetTable} As main_table({InputCols}) VALUES ({Values}) ON CONFLICT ({ConflictCols}) DO UPDATE SET {IncrementFormat};'''\
            .format(','.join(map(str,values)),{UpdateCol})
            cur.execute(query)
    except Exception as error:
        print(error)

aggTransformer()


















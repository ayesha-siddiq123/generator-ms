import configparser
import os
from sqlalchemy import create_engine
import pandas as pd
from urllib.parse import quote
from db import *
con,cur = db_connection() 

def aggTransformer(valueCols={ValueCols}):
    df_dataset = pd.read_sql('select * from {Table};',con=con)
    df_dimension = pd.read_sql('select {DimensionCols} from {DimensionTable}', con=con)
    dataset_dimension_merge = df_dataset.merge(df_dimension, on=['{MergeOnCol}'], how='inner')
    df_agg = dataset_dimension_merge.groupby({GroupBy}, as_index=False).agg({AggCols})
    df_agg['percentage'] = ((df_agg['{NumeratorCol}'] / df_agg['{DenominatorCol}']) * 100)  ### Calculating Percentage
    {DatasetCasting}
    col_list = df_agg.columns.to_list()
    df_snap = df_agg[col_list]
    df_snap.columns = valueCols
    try:
         for index,row in df_snap.iterrows():
            values = []
            for i in  valueCols:
              values.append(row[i])
            query = ''' INSERT INTO {TargetTable}({InputCols}) VALUES ({Values}) ON CONFLICT ({ConflictCols}) DO UPDATE SET {ReplaceFormat};'''\
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

aggTransformer()



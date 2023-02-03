import os
import pandas as pd
import configparser
from urllib.parse import quote
from sqlalchemy import create_engine
from db import *
con,cur = db_connection() 

def filterTransformer(valueCols={ValueCols}):
    df_dataset  = pd.read_sql('select * from {Table}', con=con)                                     ### reading dataset from database
    df_dimension=pd.read_sql('select {DimensionCols} from {DimensionTable}',con=con)                ### reading DimensionDataset from Database
    dataset_dimension_merge = df_dataset.merge(df_dimension, on=['{MergeOnCol}'], how='inner')                 ### mapping dataset with dimension
    df_total = dataset_dimension_merge.groupby({GroupBy}, as_index=False).agg({AggCols})            ### aggregation before filter

    df_filter = dataset_dimension_merge.loc[df_datasetdf_dataset['{FilterCol}']{FilterType}{Filter}]                 ### applying filter
    df_filter= df_filter.groupby({GroupBy}, as_index=False).agg({AggCols})                    ### aggregation after filter
    df_agg = df_filter.merge(df_total, on={GroupBy}, how='inner')                       ### merging aggregated DataFrames
    agg_col_list = df_agg.columns.to_list()
    numerator=agg_col_list[-2]
    denominator = agg_col_list[-1]
    df_agg['percentage'] = ((df_agg[numerator] / df_agg[denominator]) * 100)  ### Calculating Percentage
    {DatasetCasting}
    col_list=df_agg.columns.to_list()
    df_snap = df_agg[col_list]
    df_snap.column = valueCols
    try:
        for index, row in df_snap.iterrows():
            values = []
            for i in valueCols:
                values.append(row[i])
            query = ''' INSERT INTO {TargetTable}({InputCols}) VALUES ({Values}) ON CONFLICT ({ConflictCols}) DO UPDATE SET {ReplaceFormat};'''.format(','.join(map(str, values)))
            cur.execute(query)
            con.commit()
    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if con is not None:
            con.close()

filterTransformer()





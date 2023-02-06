import pandas as pd
from db_connection import *
from file_tracker_status import *
from datetime import date

con,cur=db_connection()

def filterTransformer(valueCols={ValueCols}):
    df_data = pd.read_sql('select * from {Table}', con=con)                                     ### reading dataset from database
    {DateFilter}
    {YearFilter}
    {DatasetCasting}
    df_dimension=pd.read_sql('select {DimensionCols} from {DimensionTable}',con=con)                ### reading DimensionDataset from Database
    dataset_dimension_merge = df_data.merge(df_dimension, on=['{MergeOnCol}'], how='inner')                 ### mapping dataset with dimension
    df_total = dataset_dimension_merge.groupby({GroupBy}, as_index=False).agg({AggCols})            ### aggregation before filter
    df_total['{DenominatorCol}'] = df_total['{AggCol}']
    df_filter = dataset_dimension_merge.loc[dataset_dimension_merge['{FilterCol}']{FilterType}{Filter}]  ### applying filter
    df_filter = df_filter.groupby({GroupBy}, as_index=False).agg({AggCols})  ### aggregation after filter
    df_filter['{NumeratorCol}'] = df_filter['{AggCol}']
    df_agg = df_filter.merge(df_total, on={GroupBy}, how='inner')  ### merging aggregated DataFrames
    df_agg['percentage'] = ((df_agg['{NumeratorCol}'] / df_agg['{DenominatorCol}']) * 100)  ### Calculating Percentage
    col_list = df_agg.columns.to_list()
    df_snap = df_agg[col_list]
    df_snap.columns = valueCols
    try:
        for index, row in df_snap.iterrows():
            values = []
            for i in valueCols:
                values.append(row[i])
            query = ''' INSERT INTO {TargetTable}({InputCols}) VALUES ({Values}) ON CONFLICT ({ConflictCols}) DO UPDATE SET {ReplaceFormat};'''.format(','.join(map(str, values)))
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

filterTransformer()





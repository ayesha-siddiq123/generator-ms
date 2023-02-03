import pandas as pd
from db_connection import *
from file_tracker_status import *
from datetime import date


con,cur=db_connection()


def aggTransformer(valueCols={ValueCols}):
    file_check('{KeyFile}','event')
    df_events = pd.read_csv("/processing_data/{KeyFile}")
    df_dataset = pd.read_sql('select * from {Table};', con=con)
    {DateFilter}
    {YearFilter}
    df_dimension = pd.read_sql('select {DimensionCols} from {DimensionTable}', con=con)
    event_dimension_merge = df_events.merge(df_dimension, on=['{MergeOnCol}'], how='inner')
    event_dimension_merge = event_dimension_merge.groupby({GroupBy}, as_index=False).agg({AggCols})
    event_dimension_merge['{RenameCol}'] = event_dimension_merge['{eventCol}']
    merge_col_list = []
    for i, j in (event_dimension_merge.columns.to_list(), df_dataset.columns.to_list()):
        if (i == j):
            merge_col_list.append(j)
    df_agg = event_dimension_merge.merge(df_dataset, on=merge_col_list, how='inner')
    df_agg['percentage'] = ((df_agg['{NumeratorCol}'] / df_agg['{DenominatorCol}']) * 100)  ### Calculating Percentage
    {DatasetCasting}
    df_snap = df_agg[valueCols]
    try:
        for index, row in df_snap.iterrows():
            values = []
            for i in valueCols:
                values.append(row[i])
            query = ''' INSERT INTO {TargetTable} As main_table({InputCols}) VALUES ({Values}) ON CONFLICT ({ConflictCols}) DO UPDATE SET {IncrementFormat},percentage=(({QueryNumerator})/({QueryDenominator}))*100;'''.format(','.join(map(str, values)),{UpdateCols})
            cur.execute(query)
        status_track('{KeyFile}', 'event', 'Completed_{DatasetName}')

    except Exception as error:
        print(error)

aggTransformer()

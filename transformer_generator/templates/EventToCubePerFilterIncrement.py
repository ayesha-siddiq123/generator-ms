import pandas as pd
import  os
from db_connection import *
from file_tracker_status import *
con,cur=db_connection()


def filterTransformer(valueCols={ValueCols}):
    create_folder('/processing')
    file_check({KeyFile},'event')
    df_events = pd.read_csv(os.path.dirname(path) + "/processing/" + {KeyFile})
    df_dimension = pd.read_sql('select {DimensionCols} from {DimensionTable}',con=con)  ### reading DimensionDataset from Database
    event_dimension_merge = df_events.merge(df_dimension, on=['{MergeOnCol}'],how='inner')  ### mapping dataset with dimension
    df_total = event_dimension_merge.groupby({GroupBy}, as_index=False).agg({AggCols})  ### aggregation before filter

    df_filter = event_dimension_merge.loc[event_dimension_merge['{FilterCol}']{FilterType}{Filter}]  ### applying filter
    df_filter = df_filter.groupby({GroupBy}, as_index=False).agg({AggCols})  ### aggregation after filter

    df_agg = df_filter.merge(df_total, on={GroupBy}, how='inner')  ### merging aggregated DataFrames
    agg_col_list=df_agg.columns.to_list()
    numerator = agg_col_list[-2]
    denominator = agg_col_list[-1]
    df_agg['percentage'] = ((df_agg[numerator] / df_agg[denominator]) * 100)  ### Calculating Percentage
    {DatasetCasting}    ### adding quotes to string values
    col_list = df_agg.columns.to_list()
    df_snap = df_agg[col_list]
    df_snap.columns = valueCols ### renaming dataset columns
    try:
        for index, row in df_snap.iterrows():
            values = []
            for i in valueCols:
                values.append(row[i])
            query = ''' INSERT INTO {TargetTable} As main_table({InputCols}) VALUES ({Values}) ON CONFLICT ({ConflictCols}) DO UPDATE SET {IncrementFormat},percentage=(({QueryNumerator})/({QueryDenominator}))*100;'''.format(','.join(map(str, values)),{UpdateCols})
            cur.execute(query)
            status_track({KeyFile}, 'event', 'Completed_{DatasetName}')

    except Exception as error:
        print(error)

filterTransformer()





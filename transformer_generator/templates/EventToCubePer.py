import os
import pandas as pd
from db_connection import *
from file_tracker_status import *
con,cur=db_connection()


def aggTransformer(valueCols={ValueCols}):
    create_folder('/processing')
    file_check({KeyFile},'event')
    df_events = pd.read_csv(os.path.dirname(path) + "/processing/" + {KeyFile})
    df_dimension = pd.read_sql('select {DimensionCols} from {DimensionTable}', con=con)
    event_dimension_merge = df_events.merge(df_dimension, on=['{MergeOnCol}'], how='inner')
    df_agg = event_dimension_merge.groupby({GroupBy}, as_index=False).agg({AggCols})
    df_agg['{NumeratorCol}']=df_agg['{AggColOne}']
    df_agg['{DenominatorCol}']=df_agg['{AggColTwo}']
    df_agg['percentage'] = ((df_agg['{NumeratorCol}'] / df_agg['{DenominatorCol}']) * 100)  ### Calculating Percentage
    {DatasetCasting}
    df_snap = df_agg[valueCols]
    try:
         for index,row in df_snap.iterrows():
            values = []
            for i in valueCols:
              values.append(row[i])
            query = ''' INSERT INTO {TargetTable}({InputCols}) VALUES ({Values}) ON CONFLICT ({ConflictCols}) DO UPDATE SET {ReplaceFormat};'''\
            .format(','.join(map(str,values)))
            cur.execute(query)
            status_track({KeyFile}, 'event', 'Completed_{DatasetName}')
    except Exception as error:
        print(error)

aggTransformer()


















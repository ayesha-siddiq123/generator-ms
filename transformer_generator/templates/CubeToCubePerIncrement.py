import pandas as pd
from db_connection import *
from file_tracker_status import *

con,cur=db_connection()

def aggTransformer(valueCols={ValueCols}):
    df_dataset = pd.read_sql('select * from {Table};',con=con)
    df_dimension = pd.read_sql('select {DimensionCols} from {DimensionTable}', con=con)
    dataset_dimension_merge = df_dataset.merge(df_dimension, on=['{MergeOnCol}'], how='inner')
    df_agg = dataset_dimension_merge.groupby({GroupBy}, as_index=False).agg({AggCols})
    df_agg['percentage'] = ((df_agg['{NumeratorCol}'] / df_agg['{DenominatorCol}']) * 100)  ### Calculating Percentage
    {DatasetCasting}
    df_snap = df_agg[valueCols]
    try:
         for index,row in df_snap.iterrows():
            values = []
            for i in  valueCols:
              values.append(row[i])
            query = ''' INSERT INTO {TargetTable} As main_table({InputCols}) VALUES ({Values}) ON CONFLICT ({ConflictCols}) DO UPDATE SET {IncrementFormat},percentage=(({QueryNumerator})/({QueryDenominator}))*100;'''\
            .format(','.join(map(str,values)),{UpdateCols})
            cur.execute(query)
            status_track({KeyFile}, 'event', 'Completed_{DatasetName}')

    except Exception as error:
        print(error)

aggTransformer()


